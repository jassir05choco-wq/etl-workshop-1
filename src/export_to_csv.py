import sqlite3
import pandas as pd
import os

DB_PATH = "../data/processed/recruitment_dw.db"
OUTPUT_DIR = "../data/processed/powerbi_export"

def export_tables():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    tables = [
        "dim_candidate",
        "dim_date",
        "dim_country",
        "dim_seniority",
        "dim_technology",
        "fact_applications",
    ]

    print("=" * 60)
    print("  EXPORTING DW TABLES TO CSV (for Power BI)")
    print("=" * 60)

    for table in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        filepath = f"{OUTPUT_DIR}/{table}.csv"
        df.to_csv(filepath, index=False)
        print(f"  {table}.csv  ->  {len(df):,} rows exported")

    conn.close()
    print(f"\n  All files saved in: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    export_tables()