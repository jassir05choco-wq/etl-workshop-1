"""
KPI Dashboard Generator
========================
Connects to the SQLite Data Warehouse and generates
visualizations for all 6 KPIs using SQL queries.

All data comes from the DW (star schema), NOT from the CSV.

Usage:
    Run from the project root directory:
        python src/kpis.py
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

# === CONFIGURATION ===
DB_PATH = "../data/processed/recruitment_dw.db"
OUTPUT_DIR = "../diagrams"


def get_connection():
    """Creates and returns a connection to the Data Warehouse."""
    conn = sqlite3.connect(DB_PATH)
    return conn


# ============================================================
# KPI 1: Hires by Technology
# ============================================================
def kpi_hires_by_technology(conn):
    query = """
    SELECT
        t.technology_name,
        COUNT(*) AS total_hires
    FROM fact_applications f
    JOIN dim_technology t ON f.technology_key = t.technology_key
    WHERE f.is_hired = 1
    GROUP BY t.technology_name
    ORDER BY total_hires DESC;
    """
    df = pd.read_sql_query(query, conn)
    print("\nKPI 1 - Hires by Technology:")
    print(df.to_string(index=False))

    # Visualization
    fig, ax = plt.subplots(figsize=(12, 7))
    bars = ax.barh(df["technology_name"], df["total_hires"], color="#2ecc71", edgecolor="black")
    ax.set_title("KPI 1: Hires by Technology", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Total Hires", fontsize=12)
    ax.set_ylabel("Technology", fontsize=12)
    ax.invert_yaxis()

    # Add value labels
    for bar in bars:
        width = bar.get_width()
        ax.text(width + 2, bar.get_y() + bar.get_height() / 2,
                f"{int(width)}", va="center", fontsize=9, fontweight="bold")

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/kpi1_hires_by_technology.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart saved: kpi1_hires_by_technology.png")


# ============================================================
# KPI 2: Hires by Year
# ============================================================
def kpi_hires_by_year(conn):
    query = """
    SELECT
        d.year,
        COUNT(*) AS total_hires
    FROM fact_applications f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.is_hired = 1
    GROUP BY d.year
    ORDER BY d.year;
    """
    df = pd.read_sql_query(query, conn)
    print("\nKPI 2 - Hires by Year:")
    print(df.to_string(index=False))

    # Visualization
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(df["year"].astype(str), df["total_hires"], color="#3498db", edgecolor="black", width=0.6)
    ax.set_title("KPI 2: Hires by Year", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Total Hires", fontsize=12)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, height + 10,
                f"{int(height):,}", ha="center", fontsize=11, fontweight="bold")

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/kpi2_hires_by_year.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart saved: kpi2_hires_by_year.png")


# ============================================================
# KPI 3: Hires by Seniority
# ============================================================
def kpi_hires_by_seniority(conn):
    query = """
    SELECT
        s.seniority_level,
        COUNT(*) AS total_hires
    FROM fact_applications f
    JOIN dim_seniority s ON f.seniority_key = s.seniority_key
    WHERE f.is_hired = 1
    GROUP BY s.seniority_level
    ORDER BY total_hires DESC;
    """
    df = pd.read_sql_query(query, conn)
    print("\nKPI 3 - Hires by Seniority:")
    print(df.to_string(index=False))

    # Visualization
    colors = ["#e74c3c", "#e67e22", "#f1c40f", "#2ecc71", "#3498db", "#9b59b6", "#1abc9c"]
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(df["seniority_level"], df["total_hires"], color=colors[:len(df)], edgecolor="black")
    ax.set_title("KPI 3: Hires by Seniority", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Seniority Level", fontsize=12)
    ax.set_ylabel("Total Hires", fontsize=12)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, height + 5,
                f"{int(height):,}", ha="center", fontsize=10, fontweight="bold")

    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/kpi3_hires_by_seniority.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart saved: kpi3_hires_by_seniority.png")


# ============================================================
# KPI 4: Hires by Country Over the Years
# ============================================================
def kpi_hires_by_country_year(conn):
    query = """
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
    """
    df = pd.read_sql_query(query, conn)
    print("\nKPI 4 - Hires by Country Over the Years:")
    print(df.to_string(index=False))

    # Visualization - Line chart
    fig, ax = plt.subplots(figsize=(12, 6))
    colors_map = {
        "United States of America": "#3498db",
        "Brazil": "#2ecc71",
        "Colombia": "#f1c40f",
        "Ecuador": "#e74c3c",
    }

    for country in df["country_name"].unique():
        country_data = df[df["country_name"] == country]
        ax.plot(
            country_data["year"].astype(str),
            country_data["total_hires"],
            marker="o",
            linewidth=2.5,
            markersize=8,
            label=country,
            color=colors_map.get(country, "#95a5a6"),
        )
        # Add value labels on each point
        for _, row in country_data.iterrows():
            ax.text(str(row["year"]), row["total_hires"] + 0.3,
                    str(int(row["total_hires"])), ha="center", fontsize=9, fontweight="bold")

    ax.set_title("KPI 4: Hires by Country Over the Years", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Total Hires", fontsize=12)
    ax.legend(fontsize=11, loc="upper left")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/kpi4_hires_by_country_year.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart saved: kpi4_hires_by_country_year.png")


# ============================================================
# KPI 5: Hiring Rate (%) by Technology
# ============================================================
def kpi_hiring_rate_by_technology(conn):
    query = """
    SELECT
        t.technology_name,
        COUNT(*) AS total_applications,
        SUM(f.is_hired) AS total_hires,
        ROUND(SUM(f.is_hired) * 100.0 / COUNT(*), 2) AS hiring_rate_pct
    FROM fact_applications f
    JOIN dim_technology t ON f.technology_key = t.technology_key
    GROUP BY t.technology_name
    ORDER BY hiring_rate_pct DESC;
    """
    df = pd.read_sql_query(query, conn)
    print("\nKPI 5 - Hiring Rate (%) by Technology:")
    print(df.to_string(index=False))

    # Visualization
    fig, ax = plt.subplots(figsize=(12, 7))
    colors = ["#2ecc71" if x >= 15 else "#e74c3c" if x < 10 else "#f39c12" for x in df["hiring_rate_pct"]]
    bars = ax.barh(df["technology_name"], df["hiring_rate_pct"], color=colors, edgecolor="black")
    ax.set_title("KPI 5: Hiring Rate (%) by Technology", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Hiring Rate (%)", fontsize=12)
    ax.set_ylabel("Technology", fontsize=12)
    ax.invert_yaxis()

    for bar in bars:
        width = bar.get_width()
        ax.text(width + 0.2, bar.get_y() + bar.get_height() / 2,
                f"{width:.1f}%", va="center", fontsize=9, fontweight="bold")

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/kpi5_hiring_rate_by_technology.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart saved: kpi5_hiring_rate_by_technology.png")


# ============================================================
# KPI 6: Average Scores by Seniority
# ============================================================
def kpi_avg_scores_by_seniority(conn):
    query = """
    SELECT
        s.seniority_level,
        ROUND(AVG(f.code_challenge_score), 2) AS avg_code_challenge,
        ROUND(AVG(f.technical_interview_score), 2) AS avg_technical_interview
    FROM fact_applications f
    JOIN dim_seniority s ON f.seniority_key = s.seniority_key
    GROUP BY s.seniority_level
    ORDER BY s.seniority_level;
    """
    df = pd.read_sql_query(query, conn)
    print("\nKPI 6 - Average Scores by Seniority:")
    print(df.to_string(index=False))

    # Visualization - Grouped bar chart
    fig, ax = plt.subplots(figsize=(12, 6))
    x = range(len(df))
    width = 0.35

    bars1 = ax.bar([i - width / 2 for i in x], df["avg_code_challenge"],
                   width, label="Avg Code Challenge", color="#9b59b6", edgecolor="black")
    bars2 = ax.bar([i + width / 2 for i in x], df["avg_technical_interview"],
                   width, label="Avg Technical Interview", color="#f39c12", edgecolor="black")

    ax.set_title("KPI 6: Average Scores by Seniority", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Seniority Level", fontsize=12)
    ax.set_ylabel("Average Score", fontsize=12)
    ax.set_xticks(list(x))
    ax.set_xticklabels(df["seniority_level"], rotation=45, ha="right")
    ax.legend(fontsize=11)
    ax.set_ylim(0, 7)

    # Add value labels
    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                f"{bar.get_height():.2f}", ha="center", fontsize=9, fontweight="bold")
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                f"{bar.get_height():.2f}", ha="center", fontsize=9, fontweight="bold")

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/kpi6_avg_scores_by_seniority.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Chart saved: kpi6_avg_scores_by_seniority.png")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("  KPI DASHBOARD - Recruitment Data Warehouse")
    print("=" * 60)

    conn = get_connection()

    kpi_hires_by_technology(conn)
    kpi_hires_by_year(conn)
    kpi_hires_by_seniority(conn)
    kpi_hires_by_country_year(conn)
    kpi_hiring_rate_by_technology(conn)
    kpi_avg_scores_by_seniority(conn)

    conn.close()

    print("\n" + "=" * 60)
    print("  All KPI charts saved in /diagrams folder!")
    print("=" * 60)


if __name__ == "__main__":
    main()
