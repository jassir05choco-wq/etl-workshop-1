"""
Load Module
===========
Creates the SQLite Data Warehouse and loads all dimension
and fact tables, ensuring referential integrity.
This is the third step of the ETL pipeline.
"""

import sqlite3
import pandas as pd
import os


def create_database(db_path: str, sql_path: str) -> sqlite3.Connection:
    """
    Creates the SQLite database and executes the DDL script
    to create all tables (dimensions + fact).

    Parameters:
        db_path (str): Path where the .db file will be created.
        sql_path (str): Path to the SQL file with CREATE TABLE statements.

    Returns:
        sqlite3.Connection: Active database connection.
    """
    # Remove existing database to start fresh
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"  Previous database removed.")

    conn = sqlite3.connect(db_path)

    # Enable foreign key enforcement in SQLite
    conn.execute("PRAGMA foreign_keys = ON;")

    # Read and execute the DDL script
    with open(sql_path, "r") as f:
        sql_script = f.read()

    conn.executescript(sql_script)
    print(f"  Database created: {db_path}")
    print(f"  Tables created from: {sql_path}")

    return conn


def load_dimension(conn: sqlite3.Connection, df: pd.DataFrame, table_name: str):
    """
    Loads a dimension table into the Data Warehouse.

    Parameters:
        conn: Active SQLite connection.
        df: DataFrame with the dimension data.
        table_name: Name of the target table in the DW.
    """
    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"  {table_name}: {len(df):,} records loaded")


def load_fact(conn: sqlite3.Connection, df: pd.DataFrame, table_name: str):
    """
    Loads the fact table into the Data Warehouse.

    Parameters:
        conn: Active SQLite connection.
        df: DataFrame with the fact data.
        table_name: Name of the target table in the DW.
    """
    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"  {table_name}: {len(df):,} records loaded")


def validate_load(conn: sqlite3.Connection):
    """
    Validates that all tables were loaded correctly by counting rows.
    """
    print("\n  Validation - Row counts:")
    tables = [
        "dim_candidate",
        "dim_date",
        "dim_country",
        "dim_seniority",
        "dim_technology",
        "fact_applications",
    ]
    for table in tables:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"    {table}: {count:,} rows")


def load_data(tables: dict, db_path: str, sql_path: str):
    """
    Main load function. Orchestrates the full load process.

    Parameters:
        tables (dict): Dictionary with all DataFrames (dimensions + fact).
        db_path (str): Path for the SQLite database file.
        sql_path (str): Path to the DDL SQL script.
    """
    print("=" * 60)
    print("STEP 3: LOAD")
    print("=" * 60)

    # 1. Create database and tables
    print("\n[1/3] Creating database and tables...")
    conn = create_database(db_path, sql_path)

    # 2. Load dimension tables FIRST (before fact table)
    print("\n[2/3] Loading dimension tables...")
    load_dimension(conn, tables["dim_candidate"], "dim_candidate")
    load_dimension(conn, tables["dim_date"], "dim_date")
    load_dimension(conn, tables["dim_country"], "dim_country")
    load_dimension(conn, tables["dim_seniority"], "dim_seniority")
    load_dimension(conn, tables["dim_technology"], "dim_technology")

    # 3. Load fact table AFTER dimensions (referential integrity)
    print("\n[3/3] Loading fact table...")
    load_fact(conn, tables["fact_applications"], "fact_applications")

    # 4. Validate
    validate_load(conn)

    conn.close()
    print(f"\n  Load completed successfully!")
    print(f"  Data Warehouse saved at: {db_path}\n")
