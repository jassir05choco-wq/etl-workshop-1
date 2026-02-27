"""
Main ETL Pipeline Orchestrator
===============================
Executes the full ETL process in order:
    1. Extract  → Read raw CSV
    2. Transform → Clean, apply rules, build star schema
    3. Load     → Insert into SQLite Data Warehouse

Usage:
    Run from the project root directory:
        python src/main.py
"""

from extract import extract_data
from transform import transform_data
from load import load_data


def run_pipeline():
    """
    Runs the complete ETL pipeline.
    """
    print("\n")
    print("*" * 60)
    print("   ETL PIPELINE - Recruitment Data Warehouse")
    print("*" * 60)
    print()

    # === CONFIGURATION ===
    RAW_CSV_PATH = "../data/raw/candidates.csv"
    SQL_DDL_PATH = "../sql/create_tables.sql"
    DB_PATH = "../data/processed/recruitment_dw.db"

    # === STEP 1: EXTRACT ===
    raw_data = extract_data(RAW_CSV_PATH)

    # === STEP 2: TRANSFORM ===
    tables = transform_data(raw_data)

    # === STEP 3: LOAD ===
    load_data(tables, DB_PATH, SQL_DDL_PATH)

    print("*" * 60)
    print("   ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("*" * 60)
    print()


if __name__ == "__main__":
    run_pipeline()
