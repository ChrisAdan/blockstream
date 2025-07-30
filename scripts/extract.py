import requests
import json
from datetime import datetime, timezone, timedelta
import duckdb
from duckdb import ConstraintException
import time
import random
from pathlib import Path
import sys
import re

"""
Binance.US 24hr Ticker Data Extractor

This script fetches the 24-hour ticker statistics from the Binance.US API,
filters out unwanted tickers (e.g., duplicates like XRPUSD),
saves the raw JSON response locally, and writes the data into a DuckDB
database partitioned by date.

Features:
- Retries and rate-limit handling with exponential backoff.
- Exclude list support for problematic tickers.
- Backfill capability over a specified date range.
- Incremental loading prevention via primary key checks.
"""

script_dir = Path(__file__).resolve().parent

DB = (script_dir / '..' / "data" / "blockstream.duckdb").resolve()
JSON_DIR = (script_dir / '..' / "data" / "raw_json" / "binance_us").resolve()

EXCLUDE_TICKERS = {"XRPUSD"}  # Set of ticker symbols to exclude from storage due to duplication issues


def ensure_table_exists(duckdb_path, schema="raw"):
    """
    Ensure the target DuckDB schema and table exist.

    Args:
        duckdb_path (Path or str): Path to the DuckDB database file.
        schema (str, optional): Schema name. Defaults to "raw".
    """
    con = duckdb.connect(duckdb_path)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    table_name = f"{schema}.raw_binance_us_24hr_ticker"
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT PRIMARY KEY,
            raw_response VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.close()


def exists_for_date(duckdb_path, id, schema="raw"):
    """
    Check if a record with the given id already exists in the DuckDB table.

    Args:
        duckdb_path (Path or str): Path to the DuckDB database file.
        id (int or str): The id to check (formatted date string YYYYMMDD).
        schema (str, optional): Schema name. Defaults to "raw".

    Returns:
        bool: True if record exists, False otherwise.
    """
    con = duckdb.connect(duckdb_path)
    table_name = f"{schema}.raw_binance_us_24hr_ticker"
    result = con.execute(f"SELECT COUNT(1) FROM {table_name} WHERE id = ?", [id]).fetchone()[0]
    con.close()
    return result > 0


def filter_excluded_tickers(data, exclude_set=EXCLUDE_TICKERS):
    """
    Filter out ticker entries whose symbol is in the exclude list.

    Args:
        data (list of dict): List of ticker data dictionaries.
        exclude_set (set of str, optional): Set of symbols to exclude.

    Returns:
        list of dict: Filtered ticker data.
    """
    return [item for item in data if item.get("symbol") not in exclude_set]


def fetch_binance_24hr_ticker():
    """
    Fetch the 24-hour ticker data from Binance.US API.

    Implements retry logic with exponential backoff and jitter on rate limiting,
    and exits on IP ban detection.

    Returns:
        list of dict: Parsed JSON data containing ticker information.
    """
    url = "https://api.binance.us/api/v3/ticker/24hr"
    retry_count = 0

    while True:
        try:
            response = requests.get(url)
        except requests.RequestException as e:
            print(f"[Network Error] {e}. Retrying in 10s...")
            time.sleep(10)
            continue

        if response.status_code == 200:
            print(f"[Success] Retrieved ticker data after {retry_count} retries.")
            return response.json()

        elif response.status_code == 429:
            retry_count += 1
            wait = min((2 ** retry_count) + random.uniform(1, 2), 60)  # capped exponential backoff with jitter
            print(f"[429] Rate limited. Retry {retry_count}. Waiting {wait:.2f}s...")
            time.sleep(wait)

        elif response.status_code == 418:
            msg = response.text
            match = re.search(r'until (\d+)', msg)
            if match:
                ban_until_ms = int(match.group(1))
                ban_until_dt = datetime.fromtimestamp(ban_until_ms / 1000).astimezone()
                print(f"🚫 IP banned until: {ban_until_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} (local time)")
            else:
                print("🚫 IP banned (HTTP 418), but couldn't parse expiration time.")
            sys.exit(1)

        else:
            print(f"[Error] Unexpected status {response.status_code}: {response.text}. Retrying in 15s...")
            time.sleep(15)


def save_raw_json(data, date_str, base_dir=JSON_DIR):
    """
    Save the raw ticker JSON data to a file named by date.

    Args:
        data (list of dict): Raw ticker data.
        date_str (str): Date string in "YYYYMMDD" format.
        base_dir (Path or str, optional): Directory to save file. Defaults to JSON_DIR.
    """
    base_dir = Path(base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    filename = f"daily_24hr_ticker_{date_str}.json"
    filepath = base_dir / filename

    with filepath.open("w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved raw data to {filepath}")


def write_raw_to_duckdb(data, date_str, schema="raw", duckdb_path=DB):
    """
    Insert the raw JSON data into the DuckDB table.

    Args:
        data (list of dict): Raw ticker data.
        date_str (str): Date string used as primary key.
        schema (str, optional): Schema name. Defaults to "raw".
        duckdb_path (Path or str, optional): Path to DuckDB database file.
    """
    con = duckdb.connect(duckdb_path)
    table_name = f"{schema}.raw_binance_us_24hr_ticker"
    try:
        con.execute(f"""
            INSERT INTO {table_name} (id, raw_response, created_at)
            VALUES (?, ?, ?)
        """, (date_str, json.dumps(data), datetime.now(timezone.utc)))
        print(f"✅ Inserted raw data into DuckDB schema '{schema}' with id={date_str}")
    except duckdb.ConstraintException as e:
        if "primary key" in str(e).lower():
            print(f"⚠️ Skipped insert: record with id={date_str} already exists in {table_name}.")
        else:
            raise
    finally:
        con.close()


def fetch_filter_save_write(date_str, duckdb_path=DB, base_dir=JSON_DIR):
    """
    Fetch ticker data, filter excluded tickers, save raw JSON, and write to DuckDB.

    Args:
        date_str (str): Date string in "YYYYMMDD" format.
        duckdb_path (Path or str): Path to DuckDB database file.
        base_dir (Path or str): Directory to save raw JSON files.
    """
    print(f"  ⏳ Fetching and processing data for {date_str}...")
    data = fetch_binance_24hr_ticker()
    data = filter_excluded_tickers(data)
    save_raw_json(data, date_str, base_dir=base_dir)
    write_raw_to_duckdb(data=data, date_str=date_str, duckdb_path=duckdb_path)
    print(f"  ✅ Completed processing for {date_str}")


def backfill_range(start_date, end_date, duckdb_path=DB):
    """
    Backfill Binance.US 24hr ticker data over a date range.

    For each date in the range, if data is not already present in DuckDB,
    it fetches, filters, saves, and writes the data.

    Args:
        start_date (datetime): Start date (inclusive).
        end_date (datetime): End date (inclusive).
        duckdb_path (Path or str): Path to DuckDB database file.
    """
    print(f"Starting backfill from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        print(f"\nChecking {date_str}...")
        if exists_for_date(duckdb_path, date_str):
            print(f"  ✅ Data for {date_str} already exists. Skipping.")
        else:
            try:
                fetch_filter_save_write(date_str, duckdb_path)
            except Exception as e:
                print(f"  ❌ Failed on {date_str}: {e}")
        current_date += timedelta(days=1)


def main():
    """
    Main extraction workflow for the current day.

    Checks if data for today exists in DuckDB; if not, fetches, filters,
    saves, and writes it.
    """
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    print(f"Starting extraction for date: {date_str}")

    if exists_for_date(DB, date_str):
        print(f"Data for {date_str} already exists, skipping extraction.")
        return

    fetch_filter_save_write(date_str, DB, JSON_DIR)
    print("Extraction and write complete.")


if __name__ == "__main__":
    backfill = True
    ensure_table_exists(DB)
    if backfill:
        from datetime import datetime
        backfill_range(
            start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
            end_date=datetime.now(timezone.utc)
        )
    main()
