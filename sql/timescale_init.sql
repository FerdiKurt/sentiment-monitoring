CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE SCHEMA IF NOT EXISTS core;
CREATE TABLE IF NOT EXISTS core.ohlcv(
  ts timestamptz NOT NULL,
  token text NOT NULL,
  window text NOT NULL,
  o double precision, h double precision, l double precision, c double precision, v double precision,
  PRIMARY KEY(ts, token, window)
);
SELECT create_hypertable('core.ohlcv','ts', if_not_exists=>TRUE);
