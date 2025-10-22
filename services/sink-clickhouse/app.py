import os, json, time, sys
import requests
from datetime import datetime, timezone
from kafka import KafkaConsumer, errors as kerr

KAFKA = os.getenv("KAFKA_BROKERS","redpanda:9092")
CH_URL = os.getenv("CLICKHOUSE_URL","http://clickhouse:8123")
TOPICS = ["dex.swaps","erc20.transfers"]

def ts_to_ch(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z","+00:00")).astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    except Exception:
        return iso.replace("T"," ").rstrip("Z")

def insert_jsoneachrow(table, rows):
    if not rows:
        return
    payload = "\n".join([json.dumps(r, separators=(",",":")) for r in rows])
    q = f"INSERT INTO {table} FORMAT JSONEachRow"
    r = requests.post(f"{CH_URL}/?query={q}", data=payload.encode("utf-8"), timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse insert failed [{r.status_code}]: {r.text[:400]}")

def make_consumer():
    backoff = 2
    while True:
        try:
            return KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA.split(","),
                group_id="sink-clickhouse-1",
                enable_auto_commit=True,
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=0
            )
        except kerr.NoBrokersAvailable:
            print(f"No brokers available at {KAFKA}, retrying in {backoff}s…", file=sys.stderr, flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

def main():
    print("sink-clickhouse consuming", TOPICS, flush=True)
    consumer = make_consumer()
    buf_swaps, buf_trans = [], []
    last_flush = time.time()

    while True:
        try:
            msg_pack = consumer.poll(timeout_ms=1000, max_records=1000)
        except Exception as e:
            print("Kafka poll error:", repr(e), file=sys.stderr)
            time.sleep(2)
            continue

        for tp, msgs in msg_pack.items():
            for m in msgs:
                v = m.value
                if m.topic == "dex.swaps":
                    buf_swaps.append({
                        "ts": ts_to_ch(v["ts"]),
                        "chain": int(v.get("chain",1)),
                        "pool": v["pool"],
                        "protocol": v.get("protocol","UNIV3"),
                        "token0": v.get("token0",""),
                        "token1": v.get("token1",""),
                        "symbol0": v.get("symbol0",""),
                        "symbol1": v.get("symbol1",""),
                        "decimals0": int(v.get("decimals0",0)),
                        "decimals1": int(v.get("decimals1",0)),
                        "sqrtPriceX96": str(int(v.get("sqrtPriceX96","0") or 0)),
                        "price_t1_per_t0": float(v.get("price",0.0) or 0.0),
                        "usd_t0": (float(v["usd_t0"]) if v.get("usd_t0") is not None else None),
                        "usd_t1": (float(v["usd_t1"]) if v.get("usd_t1") is not None else None),
                        "amount0": float(v.get("amount0",0.0) or 0.0),
                        "amount1": float(v.get("amount1",0.0) or 0.0),
                        "usd_notional": (float(v["usd"]) if v.get("usd") is not None else None)
                    })
                else:
                    buf_trans.append({
                        "ts": ts_to_ch(v["ts"]),
                        "chain": int(v.get("chain",1)),
                        "token": v.get("token",""),
                        "symbol": v.get("symbol",""),
                        "decimals": int(v.get("decimals",0)),
                        "from": v.get("from",""),
                        "to": v.get("to",""),
                        "amount": float(v.get("amount",0.0) or 0.0),
                        "usd": (float(v["usd"]) if v.get("usd") is not None else None)
                    })

        if time.time() - last_flush > 5 or len(buf_swaps) >= 1000 or len(buf_trans) >= 1000:
            try:
                if buf_swaps:
                    n = len(buf_swaps)
                    insert_jsoneachrow("defi.swaps_v2", buf_swaps)
                    print(f"ClickHouse insert OK: swaps_v2 +{n}")
                    buf_swaps.clear()
                if buf_trans:
                    n = len(buf_trans)
                    insert_jsoneachrow("defi.transfers_v2", buf_trans)
                    print(f"ClickHouse insert OK: transfers_v2 +{n}")
                    buf_trans.clear()
                last_flush = time.time()
            except Exception as e:
                print("ClickHouse insert error:", repr(e), file=sys.stderr)
                time.sleep(2)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
