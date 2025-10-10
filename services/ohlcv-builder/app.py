import os, json, time, sys
from datetime import datetime, timezone
from kafka import KafkaConsumer
import psycopg
from psycopg.errors import OperationalError, DatabaseError

KAFKA = os.getenv("KAFKA_BROKERS","redpanda:9092")
PGURL = os.getenv("TIMESCALE_URL","postgresql://postgres:postgres@timescaledb:5432/defi")
TOPIC = "dex.swaps"
WINDOW = "1m"
STABLES = {"USDC","USDT","DAI"}

def floor_min(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0, tzinfo=timezone.utc)

# --- Postgres connection with auto-reconnect ---
_conn = None
def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS core.ohlcv(
          ts timestamptz NOT NULL,
          token text NOT NULL,
          bar_window text NOT NULL,
          o double precision, h double precision, l double precision, c double precision, v double precision,
          PRIMARY KEY(ts, token, bar_window)
        );
        """)
    conn.commit()

def pg():
    global _conn
    try:
        if _conn is None or _conn.closed:
            _conn = psycopg.connect(PGURL, autocommit=False)
            ensure_table(_conn)
        else:
            with _conn.cursor() as cur:
                cur.execute("SELECT 1;")
    except Exception:
        try:
            if _conn and not _conn.closed:
                _conn.close()
        except Exception:
            pass
        for attempt in range(1, 6):
            try:
                time.sleep(min(2 * attempt, 10))
                _conn = psycopg.connect(PGURL, autocommit=False)
                ensure_table(_conn)
                break
            except Exception as e:
                if attempt == 5:
                    print("PG reconnect failed:", repr(e), file=sys.stderr)
                    raise
    return _conn

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA.split(","),
        group_id="ohlcv-builder-1",
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=0
    )
    print("ohlcv-builder consuming from", TOPIC, flush=True)

    buckets = {}  # (ts_iso, token_symbol) -> (o,h,l,c,v_usd)
    last_flush = time.time()

    while True:
        try:
            msg_pack = consumer.poll(timeout_ms=1000, max_records=500)
        except Exception as e:
            print("Kafka poll error:", repr(e), file=sys.stderr)
            time.sleep(2)
            continue

        for tp, msgs in msg_pack.items():
            for m in msgs:
                try:
                    d = m.value
                    ts = datetime.fromisoformat(d["ts"].replace("Z","+00:00")).astimezone(timezone.utc)
                    bucket_ts = floor_min(ts).isoformat()

                    bt = d["base_token"]; qt = d["quote_token"]
                    price = float(d["price"])               # quote per base
                    amt0 = abs(float(d.get("amount0","0"))) # base amount (abs)
                    amt1 = abs(float(d.get("amount1","0"))) # quote amount (abs)
                    usd  = d.get("usd")                     # provided when quote is a stable

                    # Case A: base is stable (e.g., USDC/WETH) -> flip to non-stable token bars
                    if bt["symbol"] in STABLES and qt["symbol"] not in STABLES:
                        token_sym = qt["symbol"]          # non-stable (e.g., WETH)
                        price = (1.0 / price) if price else None  # USD per token
                        # USD notional is the stable leg (≈ amount0 in USDC)
                        if usd is None:
                            usd = amt0

                    # Case B: base non-stable, quote stable (e.g., WETH/USDC) -> keep as-is
                    elif bt["symbol"] not in STABLES and qt["symbol"] in STABLES:
                        token_sym = bt["symbol"]          # non-stable (e.g., WETH)
                        # usd already set by ingestor as abs(amount1); fallback just in case
                        if usd is None and price is not None:
                            usd = amt1

                    # Case C: neither side stable (rare for W1) -> approximate USD
                    else:
                        token_sym = bt["symbol"]
                        if usd is None and price is not None:
                            # approx in units of quote; without FX, just use |amount1|
                            usd = amt1

                    if price is None:
                        continue

                    key = (bucket_ts, token_sym)
                    if key not in buckets:
                        buckets[key] = (price, price, price, price, float(usd or 0.0))
                    else:
                        o,h,l,c,v = buckets[key]
                        buckets[key] = (o, max(h, price), min(l, price), price, v + float(usd or 0.0))
                except Exception as e:
                    print("process message error:", repr(e), file=sys.stderr)

        # periodic flush
        if time.time() - last_flush > 10 or len(buckets) > 500:
            try:
                conn = pg()
                with conn:
                    with conn.cursor() as cur:
                        for (ts_iso, token), (o,h,l,c,v) in list(buckets.items()):
                            cur.execute("""
                            INSERT INTO core.ohlcv (ts, token, bar_window, o,h,l,c,v)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (ts, token, bar_window)
                            DO UPDATE SET
                              o=excluded.o,
                              h=GREATEST(core.ohlcv.h,excluded.h),
                              l=LEAST(core.ohlcv.l,excluded.l),
                              c=excluded.c,
                              v=core.ohlcv.v + excluded.v
                            """, (ts_iso, token, WINDOW, o,h,l,c,v))
                            buckets.pop((ts_iso, token), None)
                last_flush = time.time()
            except (OperationalError, DatabaseError) as e:
                print("PG flush error (will reconnect):", repr(e), file=sys.stderr)
                global _conn
                try:
                    if _conn and not _conn.closed:
                        _conn.close()
                except Exception:
                    pass
                _conn = None
                time.sleep(2)
            except Exception as e:
                print("Unexpected flush error:", repr(e), file=sys.stderr)
                time.sleep(2)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
