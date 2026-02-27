"""
Transform Module
================
Applies business rules, creates dimension tables and the fact table.
This is the second step of the ETL pipeline.

Business Rule:
    A candidate is HIRED if:
    Code Challenge Score >= 7 AND Technical Interview Score >= 7
"""

import pandas as pd


def apply_hiring_rule(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies the hiring business rule.
    A candidate is HIRED (1) if both scores are >= 7, otherwise REJECTED (0).
    """
    df = df.copy()
    df["is_hired"] = (
        (df["Code Challenge Score"] >= 7) & (df["Technical Interview Score"] >= 7)
    ).astype(int)

    hired_count = df["is_hired"].sum()
    print(f"  Hiring rule applied:")
    print(f"    - Hired:    {hired_count:,} ({hired_count/len(df)*100:.1f}%)")
    print(f"    - Rejected: {len(df) - hired_count:,} ({(len(df)-hired_count)/len(df)*100:.1f}%)")
    return df


def handle_data_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handles null values and data quality issues.

    Assumptions:
        - Rows with null values in critical fields are dropped.
        - Text fields are stripped of extra whitespace.
        - Scores are already validated as integers (0-10).
    """
    df = df.copy()
    initial_count = len(df)

    # Check for nulls
    null_count = df.isnull().sum().sum()
    if null_count > 0:
        df = df.dropna()
        print(f"  Dropped {initial_count - len(df)} rows with null values.")
    else:
        print("  No null values found. Dataset is clean.")

    # Strip whitespace from text columns
    text_cols = ["First Name", "Last Name", "Email", "Country", "Seniority", "Technology"]
    for col in text_cols:
        df[col] = df[col].str.strip()
    print("  Text columns stripped of whitespace.")

    return df


def create_dim_candidate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the DIM_CANDIDATE dimension table.
    Each unique combination of (First Name, Last Name, Email) gets a surrogate key.
    """
    dim = (
        df[["First Name", "Last Name", "Email"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim.index += 1  # Surrogate keys start at 1
    dim.index.name = "candidate_key"
    dim = dim.reset_index()
    dim.columns = ["candidate_key", "first_name", "last_name", "email"]
    print(f"  DIM_CANDIDATE: {len(dim):,} records")
    return dim


def create_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the DIM_DATE dimension table.
    Extracts year, month, and quarter from unique application dates.
    """
    dates = df["Application Date"].drop_duplicates().sort_values().reset_index(drop=True)
    dim = pd.DataFrame()
    dim["full_date"] = dates.dt.strftime("%Y-%m-%d")
    dim["year"] = dates.dt.year
    dim["month"] = dates.dt.month
    dim["quarter"] = dates.dt.quarter
    dim.index += 1
    dim.index.name = "date_key"
    dim = dim.reset_index()
    print(f"  DIM_DATE: {len(dim):,} records")
    return dim


def create_dim_country(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the DIM_COUNTRY dimension table.
    Each unique country gets a surrogate key.
    """
    countries = df["Country"].drop_duplicates().sort_values().reset_index(drop=True)
    dim = pd.DataFrame({"country_name": countries})
    dim.index += 1
    dim.index.name = "country_key"
    dim = dim.reset_index()
    print(f"  DIM_COUNTRY: {len(dim):,} records")
    return dim


def create_dim_seniority(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the DIM_SENIORITY dimension table.
    Each unique seniority level gets a surrogate key.
    """
    levels = df["Seniority"].drop_duplicates().sort_values().reset_index(drop=True)
    dim = pd.DataFrame({"seniority_level": levels})
    dim.index += 1
    dim.index.name = "seniority_key"
    dim = dim.reset_index()
    print(f"  DIM_SENIORITY: {len(dim):,} records")
    return dim


def create_dim_technology(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the DIM_TECHNOLOGY dimension table.
    Each unique technology gets a surrogate key.
    """
    techs = df["Technology"].drop_duplicates().sort_values().reset_index(drop=True)
    dim = pd.DataFrame({"technology_name": techs})
    dim.index += 1
    dim.index.name = "technology_key"
    dim = dim.reset_index()
    print(f"  DIM_TECHNOLOGY: {len(dim):,} records")
    return dim


def create_fact_table(
    df: pd.DataFrame,
    dim_candidate: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_country: pd.DataFrame,
    dim_seniority: pd.DataFrame,
    dim_technology: pd.DataFrame,
) -> pd.DataFrame:
    """
    Creates the FACT_APPLICATIONS table by mapping natural keys
    to surrogate keys from each dimension.

    Grain: One row per candidate application.
    """
    fact = df.copy()

    # Map candidate_key
    candidate_map = dim_candidate.set_index(["first_name", "last_name", "email"])[
        "candidate_key"
    ]
    fact["candidate_key"] = fact.set_index(
        ["First Name", "Last Name", "Email"]
    ).index.map(candidate_map)

    # Map date_key
    date_map = dim_date.set_index("full_date")["date_key"]
    fact["date_key"] = fact["Application Date"].dt.strftime("%Y-%m-%d").map(date_map)

    # Map country_key
    country_map = dim_country.set_index("country_name")["country_key"]
    fact["country_key"] = fact["Country"].map(country_map)

    # Map seniority_key
    seniority_map = dim_seniority.set_index("seniority_level")["seniority_key"]
    fact["seniority_key"] = fact["Seniority"].map(seniority_map)

    # Map technology_key
    tech_map = dim_technology.set_index("technology_name")["technology_key"]
    fact["technology_key"] = fact["Technology"].map(tech_map)

    # Select only the columns needed for the fact table
    fact = fact[
        [
            "candidate_key",
            "date_key",
            "country_key",
            "seniority_key",
            "technology_key",
            "Code Challenge Score",
            "Technical Interview Score",
            "is_hired",
        ]
    ].copy()

    fact.columns = [
        "candidate_key",
        "date_key",
        "country_key",
        "seniority_key",
        "technology_key",
        "code_challenge_score",
        "technical_interview_score",
        "is_hired",
    ]

    # Add surrogate primary key
    fact.insert(0, "application_id", range(1, len(fact) + 1))

    # Validate no null keys (referential integrity check)
    null_keys = fact[
        ["candidate_key", "date_key", "country_key", "seniority_key", "technology_key"]
    ].isnull().sum()

    if null_keys.sum() > 0:
        print(f"  WARNING: Null foreign keys found:\n{null_keys[null_keys > 0]}")
    else:
        print(f"  Referential integrity check: PASSED")

    print(f"  FACT_APPLICATIONS: {len(fact):,} records")
    return fact


def transform_data(df: pd.DataFrame) -> dict:
    """
    Main transformation function. Orchestrates all transformations.

    Returns a dictionary with all dimension tables and the fact table.
    """
    print("=" * 60)
    print("STEP 2: TRANSFORMATION")
    print("=" * 60)

    # 1. Data quality
    print("\n[1/7] Handling data quality...")
    df = handle_data_quality(df)

    # 2. Apply hiring rule
    print("\n[2/7] Applying hiring business rule...")
    df = apply_hiring_rule(df)

    # 3. Create dimensions
    print("\n[3/7] Creating DIM_CANDIDATE...")
    dim_candidate = create_dim_candidate(df)

    print("\n[4/7] Creating DIM_DATE...")
    dim_date = create_dim_date(df)

    print("\n[5/7] Creating DIM_COUNTRY...")
    dim_country = create_dim_country(df)

    print("\n[6/7] Creating DIM_SENIORITY...")
    dim_seniority = create_dim_seniority(df)

    print("\n[7/7] Creating DIM_TECHNOLOGY...")
    dim_technology = create_dim_technology(df)

    # 4. Create fact table
    print("\n[FACT] Building FACT_APPLICATIONS...")
    fact = create_fact_table(
        df, dim_candidate, dim_date, dim_country, dim_seniority, dim_technology
    )

    print(f"\n  Transformation completed successfully!\n")

    return {
        "dim_candidate": dim_candidate,
        "dim_date": dim_date,
        "dim_country": dim_country,
        "dim_seniority": dim_seniority,
        "dim_technology": dim_technology,
        "fact_applications": fact,
    }
