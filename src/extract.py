"""
Extract Module
==============
Reads the raw CSV file and performs initial data type validation.
This is the first step of the ETL pipeline.
"""

import pandas as pd


def extract_data(filepath: str) -> pd.DataFrame:
    """
    Reads the candidates CSV file and validates basic structure.

    Parameters:
        filepath (str): Path to the raw CSV file.

    Returns:
        pd.DataFrame: Raw dataframe with validated data types.
    """
    print("=" * 60)
    print("STEP 1: EXTRACTION")
    print("=" * 60)

    # Read CSV - separator is semicolon (;)
    df = pd.read_csv(filepath, sep=";")
    print(f"  Rows loaded: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")

    # Validate expected columns exist
    expected_columns = [
        "First Name",
        "Last Name",
        "Email",
        "Application Date",
        "Country",
        "YOE",
        "Seniority",
        "Technology",
        "Code Challenge Score",
        "Technical Interview Score",
    ]

    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(f"  Missing columns in CSV: {missing}")
    print("  Column validation: PASSED")

    # Convert Application Date to datetime
    df["Application Date"] = pd.to_datetime(df["Application Date"])
    print("  Date conversion: PASSED")

    # Validate numeric columns
    numeric_cols = ["YOE", "Code Challenge Score", "Technical Interview Score"]
    for col in numeric_cols:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise TypeError(f"  Column '{col}' is not numeric.")
    print("  Numeric validation: PASSED")

    print(f"\n  Extraction completed successfully!\n")
    return df
