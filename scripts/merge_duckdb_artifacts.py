import duckdb
import sys
from pathlib import Path


def merge_duckdb_files(local_db_path: Path, new_db_path: Path, schema='raw', table='raw_binance_us_24hr_ticker'):
    """
    Merge data from a new DuckDB artifact file into a local DuckDB database incrementally.

    This function:
    - Ensures the target schema and table exist in the local DuckDB.
    - Attaches the new DuckDB file as a separate database.
    - Checks if the target table exists in the attached database.
    - Inserts rows from the new database into the local database using
      an "INSERT ... ON CONFLICT DO NOTHING" statement to avoid duplicates.

    Args:
        local_db_path (Path): Path to the local DuckDB database file that will be updated.
        new_db_path (Path): Path to the new DuckDB artifact file to merge from.
        schema (str, optional): The schema name where the table resides. Defaults to 'raw'.
        table (str, optional): The table name to merge data from. Defaults to 'raw_binance_us_24hr_ticker'.

    Raises:
        SystemExit: Exits the script with error code 1 if merging encounters an exception.
    """
    con = duckdb.connect(local_db_path)

    try:
        # Ensure the schema and table exist locally
        con.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {schema};
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                id INT PRIMARY KEY,
                raw_response VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Attach the new DB to use its data
        con.execute(f"ATTACH DATABASE '{new_db_path}' AS newdb")

        # Check if table exists in attached DB
        table_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = '{schema}' AND table_name = '{table}'
        """).fetchone()[0]

        if not table_exists:
            print(f"⚠️ Table {schema}.{table} not found in {new_db_path.name}. Skipping.")
            return

        # Perform bulk insert with conflict handling
        result = con.execute(f"""
            INSERT INTO {schema}.{table} (id, raw_response, created_at)
            SELECT id, raw_response, created_at
            FROM newdb.{schema}.{table}
            ON CONFLICT(id) DO NOTHING
            RETURNING *;
        """).fetchall()
        print(f"✅ Inserted {len(result)} new rows from {new_db_path.name} into {local_db_path.name}")

    except Exception as e:
        print(f"❌ Error during merging: {e}")
        sys.exit(1)

    finally:
        con.close()

    print(f"✅ Merged new rows from {new_db_path.name} into {local_db_path.name}")


if __name__ == "__main__":
    """
    Script entry point to merge DuckDB artifact files.

    Expects exactly two command-line arguments:
    1) Path to the local DuckDB database file.
    2) Path to the new DuckDB artifact file to merge.

    Usage:
        python merge_duckdb_artifacts.py <local_db_path> <new_db_path>
    """
    if len(sys.argv) != 3:
        print("Usage: python merge_duckdb_artifacts.py <local_db_path> <new_db_path>")
        sys.exit(1)

    local_db_path = Path(sys.argv[1])
    new_db_path = Path(sys.argv[2])

    merge_duckdb_files(local_db_path, new_db_path)
