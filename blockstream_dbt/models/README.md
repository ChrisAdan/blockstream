# 🧱 BlockStream: dbt Transformation & Data Architecture Framework

[![DuckDB](https://img.shields.io/badge/Database-DuckDB-yellowgreen)](https://duckdb.org)  
[![dbt](https://img.shields.io/badge/dbt-analytics%20engineering-orange)](https://www.getdbt.com)  
[![Status](https://img.shields.io/badge/status-in%20design-blue)]()  
[![Coverage](https://img.shields.io/badge/tests-auto%20&%20custom%20in%20dbt-green)]()  
[![Monitoring](https://img.shields.io/badge/monitoring-enabled-success)]()

---

## 🔧 Overview

A robust framework for transforming Binance USDT 24hr ticker dailies using dbt and DuckDB, featuring:

- Clean transformation pipeline (bronze → staging → intermediate → gold)
- Rolling crypto-finance metrics (7/14/28d)
- Diagnostic & ingestion health models
- Automated testing, documentation, and monitoring
- Designed for extensibility and reliability

---

## 🧱 1. Data Architecture

### ❄️ Bronze Layer: `raw__24hr_ticker_usdt`

**Source**: Binance 24hr Ticker  
**Grain**: One row per symbol per snapshot  
**Storage Format**: DuckDB (optionally `raw_json` as VARIANT)

#### Schema

| Column             | Type      | Description                     |
| ------------------ | --------- | ------------------------------- |
| `symbol`           | TEXT      | Trading pair (e.g., BTCUSDT)    |
| `price_change`     | FLOAT     | Price change over 24hr          |
| `price_change_pct` | FLOAT     | Percent change over 24hr        |
| `high_price`       | FLOAT     | 24hr high                       |
| `low_price`        | FLOAT     | 24hr low                        |
| `volume`           | FLOAT     | Volume traded in base currency  |
| `quote_volume`     | FLOAT     | Volume traded in quote currency |
| `open_time`        | TIMESTAMP | 24hr window open                |
| `close_time`       | TIMESTAMP | 24hr window close               |
| `load_ts`          | TIMESTAMP | Ingestion timestamp             |
| `raw_json`         | VARIANT   | Full raw record (optional)      |

---

## 🧪 2. dbt Models

### 🥈 Staging Layer: `stg__24hr_ticker_usdt`

- Type enforcement, renaming, filtering
- Adds `date` column from `open_time`
- Deduplication logic

---

### 📊 Intermediate Layer: `int__ticker_rolling_metrics`

- Rolling metrics:
  - 7/14/28-day high/low prices
  - Rolling average price change %
  - Volatility (stddev of price_change_pct)
  - Volume trends
- Uses window functions

---

### 🪙 Gold: `dim__coins`

- Dimension table of observed coins
- Enrich with:
  - First/last seen timestamp
  - Coin status (active/delisted)

---

### 📈 Gold: `fact__ticker_summary_daily`

- One row per `symbol` per `date`
- Metrics:
  - Daily change %, daily volume
  - Joined rolling stats
  - Enriched coin info

---

### 🩺 Diagnostics: `meta__ticker_load_stats`

- Daily load health metrics

#### Schema

| Column            | Type    | Description                           |
| ----------------- | ------- | ------------------------------------- |
| `date`            | DATE    | Snapshot date                         |
| `record_count`    | INTEGER | Number of records ingested            |
| `data_volume_mb`  | FLOAT   | Size of data ingested                 |
| `unique_symbols`  | INTEGER | Count of unique symbols               |
| `missing_symbols` | INTEGER | Count of expected-but-missing symbols |

---

## ✅ 3. Testing Strategy

### 🔹 Native dbt Tests

- `not_null`: `symbol`, `high_price`, `low_price`, `price_change_pct`
- `unique`: (`symbol`, `date`)
- `accepted_range`:
  - `price_change_pct`: -100 to 1000
  - `volume`: >= 0

### 🔸 Custom Tests

- `test_complete_coin_set`: All expected coins present
- `test_no_price_reversal`: Ensure `high_price >= low_price`

---

## 📚 4. Documentation

- All models described in `schema.yml`
- Each column has:
  - `description`
  - Data type
  - Optional metric definition (dbt metrics)
- Use tags:
  - `tag:gold`, `tag:diagnostics`, `tag:finance`

---

## 🔔 5. Monitoring & Alerts

### Diagnostics Model

- `meta__ticker_load_stats`
  - Daily record count
  - Data volume tracking
  - Unique/missing coins

### Notifications (Future)

- Slack/Email via GitHub Actions or Airflow
- Alert on failed dbt builds or missing data

### Optional

- `dbt-artifacts` for manifest/run results
- Historical monitoring dashboards via DuckDB or lightweight BI

---

## 🔄 6. Operational Concerns

- **Freshness check**: Snapshot is <24hr old
- **Backfill support**: Load up to 30d of historical snapshots
- **Deduplication**: Use `row_number()` over symbol/date
- **UTC enforcement**: All time data normalized to UTC

---

## 📊 7. Finance Metrics to Track

| Metric                  | Description                                   |
| ----------------------- | --------------------------------------------- |
| `rolling_high_7d`       | Max high_price over last 7 days               |
| `rolling_avg_price_14d` | Avg of (high + low)/2 over 14d                |
| `volatility_28d`        | Stddev of price_change_pct over 28 days       |
| `volume_trend_7d`       | Volume change over last 7 days vs prior 7     |
| `price_momentum_14d`    | Slope of price trend (linear regression 14d)  |
| `max_drawdown`          | % drop from peak price within trailing window |

---

## 📦 8. DuckDB Optimization Tips

- Partition data by `date`
- Materialize intermediate layers with `parquet` if large
- Enable compression
- Use `VACUUM` periodically for space cleanup
- Indexing (DuckDB auto-optimized) only if truly needed

---

## 🧭 Summary Table

| Schema             | Purpose                              | Example Models                             |
| ------------------ | ------------------------------------ | ------------------------------------------ |
| `raw`              | Ingested JSON or structured data     | `raw__24hr_ticker_usdt`                    |
| `staging`          | Typed, deduped, enriched source data | `stg__24hr_ticker_usdt`                    |
| `intermediate`     | Business logic + rolling metrics     | `int__ticker_rolling_metrics`              |
| `gold`             | Analytics-ready facts and dimensions | `fact__ticker_summary_daily`, `dim__coins` |
| `meta/diagnostics` | Data quality & ingestion health      | `meta__ticker_load_stats`                  |

---

## ✅ Next Steps

- Scaffold models & `schema.yml` files
- Implement diagnostics and monitoring
- Add Airflow or GitHub CI for orchestration
- Build dashboards for:
  - Coin stats
  - Price trends
  - Load diagnostics

---

_Designed for: Analytics Engineers & Fintech DataOps_  
_Last updated: July 2025_
