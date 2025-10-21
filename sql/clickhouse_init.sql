CREATE DATABASE IF NOT EXISTS defi;

-- v2 swaps table with richer fields
CREATE TABLE IF NOT EXISTS defi.swaps_v2
(
  ts              DateTime64(3),
  chain           UInt16,
  pool            String,
  protocol        LowCardinality(String),
  token0          String,
  token1          String,
  symbol0         LowCardinality(String),
  symbol1         LowCardinality(String),
  decimals0       UInt8,
  decimals1       UInt8,
  sqrtPriceX96    UInt128,
  price_t1_per_t0 Float64,
  usd_t0          Nullable(Float64),
  usd_t1          Nullable(Float64),
  amount0         Float64,
  amount1         Float64,
  usd_notional    Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (pool, ts)
PARTITION BY toYYYYMM(ts);

-- v2 transfers table
CREATE TABLE IF NOT EXISTS defi.transfers_v2
(
  ts        DateTime64(3),
  chain     UInt16,
  token     String,
  symbol    LowCardinality(String),
  decimals  UInt8,
  "from"    String,
  "to"      String,
  amount    Float64,
  usd       Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (token, ts)
PARTITION BY toYYYYMM(ts);
