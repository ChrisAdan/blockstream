import requests
import json
import os
from datetime import datetime, timezone, timedelta
import duckdb
import time
import random
import sys
import re

def exists_for_date(duckdb_path, id):
    """
    Check if a record for the given date ID already exists in the DuckDB table.

    Args:
        duckdb_path (str): Path to the DuckDB database file.
        id (str): Date string in 'YYYYMMDD' format serving as the primary key.

    Returns:
        bool: True if a record exists for the given id, False otherwise.
    """
    con = duckdb.connect(duckdb_path)
    result = con.execute(
        "SELECT COUNT(1) FROM raw_binance_us_24hr_ticker WHERE id = ?", [id]
    ).fetchone()[0]
    con.close()
    return result > 0

def fetch_binance_24hr_ticker():
    """
    Fetch the 24-hour ticker data from the Binance.US public API.

    Returns:
        list: A list of dictionaries containing ticker data for all trading pairs.

    Notes:
        - Retries indefinitely until a successful response is received.
        - Handles rate limiting (429) with exponential backoff and jitter.
        - Exits immediately if an IP ban (418) is encountered.
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
            wait = min((2 ** retry_count) + random.uniform(1, 2), 60)  # jitter + capped backoff
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

def save_raw_json(data, date_str, base_dir="data/raw_json/binance_us"):
    """
    Save the raw JSON data to a file named with the given date.

    Args:
        data (list): The raw data to save (typically JSON-parsed Python list/dict).
        date_str (str): The date string in 'YYYYMMDD' format for filename.
        base_dir (str, optional): Directory path where the file will be saved.
            Defaults to "data/raw_json/binance_us".
    """
    os.makedirs(base_dir, exist_ok=True)
    filename = f"daily_24hr_ticker_{date_str}.json"
    filepath = os.path.join(base_dir, filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved raw data to {filepath}")

def write_raw_to_duckdb(data, date_str, schema="raw", duckdb_path="data/blockstream.duckdb"):
    """
    Insert the raw JSON data into the DuckDB raw data table within the specified schema.

    Args:
        data (list|dict): The raw data to insert (typically JSON-parsed Python list/dict).
        date_str (str): The date string in 'YYYYMMDD' format serving as primary key.
        schema (str, optional): The target schema. Defaults to 'raw'.
        duckdb_path (str, optional): Path to the DuckDB database file. Defaults to 'data/blockstream.duckdb'.
    """
    con = duckdb.connect(duckdb_path)

    # Ensure schema exists
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # Fully qualified table name
    table_name = f"{schema}.raw_binance_us_24hr_ticker"

    # Create table if it doesn't exist
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT PRIMARY KEY,
            raw_response VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert data
    con.execute(f"""
        INSERT INTO {table_name} (id, raw_response, created_at)
        VALUES (?, ?, ?)
    """, (date_str, json.dumps(data), datetime.now(timezone.utc)))

    con.close()
    print(f"Inserted raw data into DuckDB schema '{schema}' with id={date_str}")

def backfill_range(start_date, end_date, duckdb_path="data/blockstream.duckdb"):
    """
    Backfill Binance.US 24hr ticker data from start_date to end_date (inclusive).

    Args:
        start_date (datetime): Start date (UTC).
        end_date (datetime): End date (UTC).
        duckdb_path (str): Path to DuckDB database.
    """
    print(f"Starting backfill from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        print(f"\nChecking {date_str}...")

        if exists_for_date(duckdb_path, date_str):
            print(f"  ✅ Data for {date_str} already exists. Skipping.")
        else:
            print(f"  ⏳ Fetching and writing data for {date_str}...")
            try:
                data = fetch_binance_24hr_ticker()
                save_raw_json(data, date_str)
                write_raw_to_duckdb(data, date_str, duckdb_path)
                print(f"  ✅ Backfilled {date_str}")
            except Exception as e:
                print(f"  ❌ Failed on {date_str}: {e}")

        current_date += timedelta(days=1)

def main():
    """
    Main extraction process.

    - Checks if data for today already exists in DuckDB.
    - If not, fetches 24hr ticker data from Binance.US API.
    - Saves raw JSON file locally.
    - Inserts raw data into DuckDB.

    Uses UTC date as the data partition key.
    """
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    print(f"Starting extraction for date: {date_str}")

    if exists_for_date("data/blockstream.duckdb", date_str):
        print(f"Data for {date_str} already exists, skipping extraction.")
        return

    print("Fetching Binance.US 24hr ticker data...")
    data = fetch_binance_24hr_ticker()

    save_raw_json(data, date_str)
    write_raw_to_duckdb(data, date_str)

    print("Extraction and write complete.")

if __name__ == "__main__":
    backfill = False
    if backfill:
        from datetime import datetime
        backfill_range(
            start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2025, 7, 18, tzinfo=timezone.utc)
        )
    main()
