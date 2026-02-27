-- ============================================================
-- KPI Queries - Recruitment Data Warehouse
-- All queries run against the DW (star schema), NOT the CSV.
-- ============================================================


-- ============================================================
-- KPI 1: Hires by Technology
-- ============================================================
SELECT
    t.technology_name,
    COUNT(*) AS total_hires
FROM fact_applications f
JOIN dim_technology t ON f.technology_key = t.technology_key
WHERE f.is_hired = 1
GROUP BY t.technology_name
ORDER BY total_hires DESC;


-- ============================================================
-- KPI 2: Hires by Year
-- ============================================================
SELECT
    d.year,
    COUNT(*) AS total_hires
FROM fact_applications f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.is_hired = 1
GROUP BY d.year
ORDER BY d.year;


-- ============================================================
-- KPI 3: Hires by Seniority
-- ============================================================
SELECT
    s.seniority_level,
    COUNT(*) AS total_hires
FROM fact_applications f
JOIN dim_seniority s ON f.seniority_key = s.seniority_key
WHERE f.is_hired = 1
GROUP BY s.seniority_level
ORDER BY total_hires DESC;


-- ============================================================
-- KPI 4: Hires by Country Over the Years
-- (Focused on USA, Brazil, Colombia, Ecuador)
-- ============================================================
SELECT
    c.country_name,
    d.year,
    COUNT(*) AS total_hires
FROM fact_applications f
JOIN dim_country c ON f.country_key = c.country_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.is_hired = 1
  AND c.country_name IN ('United States of America', 'Brazil', 'Colombia', 'Ecuador')
GROUP BY c.country_name, d.year
ORDER BY c.country_name, d.year;


-- ============================================================
-- KPI 5 (Additional): Hiring Rate (%) by Technology
-- ============================================================
SELECT
    t.technology_name,
    COUNT(*) AS total_applications,
    SUM(f.is_hired) AS total_hires,
    ROUND(SUM(f.is_hired) * 100.0 / COUNT(*), 2) AS hiring_rate_pct
FROM fact_applications f
JOIN dim_technology t ON f.technology_key = t.technology_key
GROUP BY t.technology_name
ORDER BY hiring_rate_pct DESC;


-- ============================================================
-- KPI 6 (Additional): Average Scores by Seniority
-- ============================================================
SELECT
    s.seniority_level,
    ROUND(AVG(f.code_challenge_score), 2) AS avg_code_challenge,
    ROUND(AVG(f.technical_interview_score), 2) AS avg_technical_interview
FROM fact_applications f
JOIN dim_seniority s ON f.seniority_key = s.seniority_key
GROUP BY s.seniority_level
ORDER BY s.seniority_level;
