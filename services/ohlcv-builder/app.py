import os, json, time, sys
from datetime import datetime, timezone
from kafka import KafkaConsumer
import psycopg
from psycopg.errors import OperationalError, DatabaseError

KAFKA = os.getenv("KAFKA_BROKERS","redpanda:9092")
PGURL = os.getenv("TIMESCALE_URL","postgresql://postgres:postgres@timescaledb:5432/defi")
TOPIC = "dex.swaps"
WINDOW = "1m"
STABLES = set([s.strip().lower() for s in os.getenv("STABLE_TOKEN_ADDRESSES","").split(",") if s.strip()])

def floor_min(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0, tzinfo=timezone.utc)

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
            with _conn.cursor() as cur: cur.execute("SELECT 1;")
    except Exception:
        try:
            if _conn and not _conn.closed: _conn.close()
        except Exception: pass
        for attempt in range(1,6):
            try:
                time.sleep(min(2*attempt,10))
                _conn = psycopg.connect(PGURL, autocommit=False)
                ensure_table(_conn)
                break
            except Exception as e:
                if attempt==5: print("PG reconnect failed:", repr(e), file=sys.stderr); raise
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

    buckets = {}
    last_flush = time.time()

    while True:
        try:
            msg_pack = consumer.poll(timeout_ms=1000, max_records=500)
        except Exception as e:
            print("Kafka poll error:", repr(e), file=sys.stderr); time.sleep(2); continue

        for tp, msgs in msg_pack.items():
            for m in msgs:
                try:
                    d = m.value
                    ts = datetime.fromisoformat(d["ts"].replace("Z","+00:00")).astimezone(timezone.utc)
                    bucket_ts = floor_min(ts).isoformat()

                    # Prefer to build bars for the non-stable token if present
                    t0 = d["token0"].lower(); t1 = d["token1"].lower()
                    sym0 = d.get("symbol0","T0"); sym1 = d.get("symbol1","T1")
                    usd0 = d.get("usd_t0"); usd1 = d.get("usd_t1")
                    amt0 = abs(float(d.get("amount0",0.0))); amt1 = abs(float(d.get("amount1",0.0)))

                    target_sym = None
                    px_usd = None
                    vol_usd = d.get("usd")

                    if t0 in STABLES and t1 not in STABLES and usd1 is not None:
                        target_sym = sym1; px_usd = float(usd1)
                        if vol_usd is None: vol_usd = amt0  # stable leg
                    elif t1 in STABLES and t0 not in STABLES and usd0 is not None:
                        target_sym = sym0; px_usd = float(usd0)
                        if vol_usd is None: vol_usd = amt1  # stable leg
                    elif usd0 is not None:
                        target_sym = sym0; px_usd = float(usd0)
                        if vol_usd is None and usd1 is not None: vol_usd = min(amt0*usd0, amt1*usd1)
                    elif usd1 is not None:
                        target_sym = sym1; px_usd = float(usd1)
                        if vol_usd is None and usd0 is not None: vol_usd = min(amt0*usd0, amt1*usd1)
                    else:
                        # skip if we cannot price in USD
                        continue

                    if vol_usd is None: vol_usd = 0.0

                    key = (bucket_ts, target_sym)
                    if key not in buckets:
                        buckets[key] = (px_usd, px_usd, px_usd, px_usd, float(vol_usd or 0.0))
                    else:
                        o,h,l,c,v = buckets[key]
                        h = max(h, px_usd)
                        l = min(l, px_usd)
                        c = px_usd
                        v += float(vol_usd or 0.0)
                        buckets[key] = (o,h,l,c,v)

                except Exception as e:
                    print("process msg error:", repr(e), file=sys.stderr)

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
                print("PG flush error (reconnect):", repr(e), file=sys.stderr)
                global _conn
                try:
                    if _conn and not _conn.closed: _conn.close()
                except Exception: pass
                _conn = None
                time.sleep(2)
            except Exception as e:
                print("Unexpected flush error:", repr(e), file=sys.stderr)
                time.sleep(2)

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: pass
