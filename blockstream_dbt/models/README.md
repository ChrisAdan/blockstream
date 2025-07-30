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

### ❄️ Bronze Layer: `raw_binance_us_24hr_ticker`

**Source**: Binance 24hr Ticker  
**Grain**: One row per symbol per snapshot  
**Storage Format**: DuckDB (optionally `raw_json` as VARIANT)

#### Schema

| Column         | Type      | Description                            |
| -------------- | --------- | -------------------------------------- |
| `id`           | TEXT      | Unique identifier for the record       |
| `raw_response` | VARIANT   | Full raw JSON response from the API    |
| `created_at`   | TIMESTAMP | Timestamp when the record was ingested |

---

## 🧪 2. dbt Models

### 🥈 Staging Layer: `stg__24hr_ticker_usdt`

- Type enforcement, renaming, filtering
- Adds `date` column from `open_time`
- Deduplication logic

---

### 📊 Intermediate Layer: `stg__binance_us_24hr_ticker`

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

This model dynamically generates rolling financial indicators using Jinja macros, allowing for scalable extension to additional metrics or time windows. Metrics include standard aggregates (avg, sum, stddev) as well as custom-calculated indicators such as true range and max drawdown. Metrics are windowed over 7, 14, and 28-day periods, making it adaptable for trend analysis and volatility tracking.

| Metric                   | Aggregations     | Windows   | Definition                                                 |
| ------------------------ | ---------------- | --------- | ---------------------------------------------------------- |
| `last_price`             | avg, stddev      | 7, 14, 28 | Average or volatility of the last trading price            |
| `high_price`             | avg, stddev      | 7, 14, 28 | Aggregated high price over time                            |
| `low_price`              | avg, stddev      | 7, 14, 28 | Aggregated low price over time                             |
| `volume`                 | avg, sum, stddev | 7, 14, 28 | Trading volume summary and trend                           |
| `quote_volume`           | avg, sum, stddev | 7, 14, 28 | Volume in quote asset terms                                |
| `trade_count`            | avg, sum, stddev | 7, 14, 28 | Number of trades in the period                             |
| `bid_qty`                | avg, stddev      | 7, 14, 28 | Average and volatility of bid-side quantity                |
| `ask_qty`                | avg, stddev      | 7, 14, 28 | Average and volatility of ask-side quantity                |
| `true_range_{window}d`   | _custom macro_   | 7, 14, 28 | High - Low of last price for each day, then rolling stddev |
| `max_drawdown_{window}d` | _custom macro_   | 7, 14, 28 | Largest peak-to-trough drop in price over the period       |
| `pct_change_7d`          | derived          | 7         | Change in last price vs 7-day average                      |
| `liquidity_ratio`        | derived          | 7         | Volume divided by 7-day average price                      |
| `price_to_volume_ratio`  | derived          | 1         | Last price divided by volume                               |
| `bid_ask_spread`         | derived          | 1         | Ask price minus bid price                                  |
| `spread_pct`             | derived          | 1         | Bid-ask spread relative to price                           |
| `price_efficiency_ratio` | derived          | 7         | Abs(price change) vs high-low range over 7d                |
| `bid_ratio_7d`           | derived          | 7         | Bid qty share of total book (7d avg)                       |
| `ask_ratio_7d`           | derived          | 7         | Ask qty share of total book (7d avg)                       |
| `rank_price_increase_7d` | rank             | 7         | Rank of tokens by 7-day price change                       |
| `rank_volatility_7d`     | rank             | 7         | Rank by price stddev over 7 days                           |
| `rank_volume_7d`         | rank             | 7         | Rank by 7-day trading volume                               |
| `rank_liquidity_7d`      | rank             | 7         | Rank by liquidity ratio                                    |
| `rank_trade_activity_7d` | rank             | 7         | Rank by number of trades in 7 days                         |

_Note: All metrics are generated dynamically via Jinja loops and custom macros to ensure minimal duplication and easy extensibility._

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
| `raw`              | Ingested JSON or structured data     | `raw_binance_us_24hr_ticker`               |
| `staging`          | Typed, deduped, enriched source data | `stg__binance_us_24hr_ticker`              |
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
