-- ============================================================
-- Star Schema DDL - Recruitment Data Warehouse
-- Database: SQLite
-- Grain: One row per candidate application
-- ============================================================

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- DIM_CANDIDATE: Stores unique candidate information
-- Surrogate Key: candidate_key (auto-increment)
CREATE TABLE IF NOT EXISTS dim_candidate (
    candidate_key INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    email         TEXT NOT NULL
);

-- DIM_DATE: Date dimension for time-based analysis
-- Surrogate Key: date_key (auto-increment)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    full_date TEXT    NOT NULL,  -- Format: YYYY-MM-DD
    year      INTEGER NOT NULL,
    month     INTEGER NOT NULL,
    quarter   INTEGER NOT NULL
);

-- DIM_COUNTRY: Stores unique country names
-- Surrogate Key: country_key (auto-increment)
CREATE TABLE IF NOT EXISTS dim_country (
    country_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name TEXT    NOT NULL
);

-- DIM_SENIORITY: Stores seniority levels
-- Surrogate Key: seniority_key (auto-increment)
CREATE TABLE IF NOT EXISTS dim_seniority (
    seniority_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    seniority_level TEXT    NOT NULL
);

-- DIM_TECHNOLOGY: Stores technology/role names
-- Surrogate Key: technology_key (auto-increment)
CREATE TABLE IF NOT EXISTS dim_technology (
    technology_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    technology_name TEXT    NOT NULL
);

-- ============================================================
-- FACT TABLE
-- ============================================================

-- FACT_APPLICATIONS: Central fact table
-- Grain: One row per candidate application
-- Measures: code_challenge_score, technical_interview_score, is_hired
CREATE TABLE IF NOT EXISTS fact_applications (
    application_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_key             INTEGER NOT NULL,
    date_key                  INTEGER NOT NULL,
    country_key               INTEGER NOT NULL,
    seniority_key             INTEGER NOT NULL,
    technology_key            INTEGER NOT NULL,
    code_challenge_score      INTEGER NOT NULL,
    technical_interview_score INTEGER NOT NULL,
    is_hired                  INTEGER NOT NULL,  -- 1 = Hired, 0 = Rejected

    FOREIGN KEY (candidate_key)  REFERENCES dim_candidate(candidate_key),
    FOREIGN KEY (date_key)       REFERENCES dim_date(date_key),
    FOREIGN KEY (country_key)    REFERENCES dim_country(country_key),
    FOREIGN KEY (seniority_key)  REFERENCES dim_seniority(seniority_key),
    FOREIGN KEY (technology_key) REFERENCES dim_technology(technology_key)
);
