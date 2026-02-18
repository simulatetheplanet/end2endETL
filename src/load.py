import os
import duckdb

# -------------------------
# Config
# -------------------------

DB_PATH = os.path.join("warehouse", "nba.duckdb")
PARQUET_PATH = os.path.join("data", "processed", "games.parquet")


# -------------------------
# Load into DuckDB
# -------------------------

def load_games():
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(f"Parquet file not found: {PARQUET_PATH}")

    os.makedirs("warehouse", exist_ok=True)

    con = duckdb.connect(DB_PATH)

    # Create or replace games table
    con.execute(f"""
        CREATE OR REPLACE TABLE games AS
        SELECT *
        FROM read_parquet('{PARQUET_PATH}');
    """)

    row_count = con.execute("SELECT COUNT(*) FROM games").fetchone()[0]

    print("Loaded data into DuckDB warehouse.")
    print(f"Rows loaded: {row_count}")
    print(f"DuckDB file: {DB_PATH}")

    con.close()


if __name__ == "__main__":
    load_games()
