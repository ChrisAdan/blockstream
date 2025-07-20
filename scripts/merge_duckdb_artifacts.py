# merge_duckdb_artifacts.py
import duckdb
import sys
from pathlib import Path

def merge_duckdb_files(local_db_path: Path, new_db_path: Path, schema='raw', table='raw_binance_us_24hr_ticker'):
    con_local = duckdb.connect(local_db_path)
    con_new = duckdb.connect(new_db_path)

    # Ensure table exists in local (create if missing, match schema)
    con_local.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id INT PRIMARY KEY,
            raw_response VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


    # Better: fetch new rows in Python and insert into local DB:
    new_rows = con_new.execute(f"SELECT id, raw_response, created_at FROM {schema}.{table}").fetchall()

    for row in new_rows:
        try:
            con_local.execute(f"""
                INSERT INTO {schema}.{table} (id, raw_response, created_at)
                VALUES (?, ?, ?)
            """, row)
        except duckdb.IOException:
            # skip duplicates
            pass

    con_local.close()
    con_new.close()
    print(f"Merged {len(new_rows)} rows from {new_db_path} into {local_db_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python merge_duckdb_artifacts.py <local_db_path> <new_db_path>")
        sys.exit(1)

    local_db_path = Path(sys.argv[1])
    new_db_path = Path(sys.argv[2])

    merge_duckdb_files(local_db_path, new_db_path)
