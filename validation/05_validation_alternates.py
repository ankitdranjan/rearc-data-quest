# Databricks notebook source
# 05_validation_alternates.py
# Rearc Data Quest - SQL/PySpark alternate implementations and parity validation

from pyspark.sql import functions as F


# COMMAND ----------
# Helper: compare two DataFrames in both directions.
# Column names, order, and compatible data types should match before calling.

def validate_parity(question_name, df_primary, df_alternate):
    primary_minus_alternate = df_primary.exceptAll(df_alternate).count()
    alternate_minus_primary = df_alternate.exceptAll(df_primary).count()

    status = (
        "PASS"
        if primary_minus_alternate == 0 and alternate_minus_primary == 0
        else "FAIL"
    )

    print(
        f"{question_name}: {status} | "
        f"primary_minus_alternate={primary_minus_alternate}, "
        f"alternate_minus_primary={alternate_minus_primary}"
    )


# COMMAND ----------
# ============================================================
# Q1
# Mean and sample standard deviation of annual US population
# from 2013 through 2018 inclusive.
#
# Primary:   Spark SQL semantic view
# Alternate: PySpark DataFrame API
# ============================================================

df_q1_primary = (
    spark.table("rearc.gold.v_us_population_stats_2013_2018")
    .select(
        "start_year",
        "end_year",
        "number_of_years",
        "mean_population",
        "stddev_population"
    )
)

df_q1_pyspark = (
    spark.table("rearc.gold.us_population_yearly")
    .filter(F.col("year").between(2013, 2018))
    .agg(
        F.min("year").alias("start_year"),
        F.max("year").alias("end_year"),
        F.count("*").alias("number_of_years"),
        F.avg("population").alias("mean_population"),
        F.stddev_samp("population").alias("stddev_population")
    )
)

print("Q1 - PySpark alternate result")
display(df_q1_pyspark)

validate_parity(
    "Q1",
    df_q1_primary,
    df_q1_pyspark
)


# COMMAND ----------
# ============================================================
# Q2
# For each BLS series_id, find the year with the largest
# SUM(value) across Q01-Q04.
#
# Primary:   PySpark Gold dataset / semantic view
# Alternate: Spark SQL
#
# Tie-breaker: if annual values tie, choose the latest year.
# ============================================================

df_q2_primary = (
    spark.table("rearc.gold.v_bls_series_best_year")
    .select(
        "series_id",
        "best_year",
        "best_year_value",
        "quarter_count"
    )
)

df_q2_sql = spark.sql("""
WITH yearly AS
(
    SELECT
        series_id,
        CAST(year AS INT) AS year,
        SUM(value) AS annual_value,
        COUNT(DISTINCT period) AS quarter_count
    FROM rearc.silver.observations
    WHERE __END_AT IS NULL
      AND period IN ('Q01', 'Q02', 'Q03', 'Q04')
    GROUP BY
        series_id,
        CAST(year AS INT)
),
ranked AS
(
    SELECT
        series_id,
        year,
        annual_value,
        quarter_count,
        ROW_NUMBER() OVER
        (
            PARTITION BY series_id
            ORDER BY annual_value DESC, year DESC
        ) AS rn
    FROM yearly
)
SELECT
    series_id,
    year AS best_year,
    annual_value AS best_year_value,
    quarter_count
FROM ranked
WHERE rn = 1
""")

print("Q2 - Spark SQL alternate result")
display(df_q2_sql.orderBy("series_id"))

validate_parity(
    "Q2",
    df_q2_primary,
    df_q2_sql
)


# COMMAND ----------
# ============================================================
# Q3
# PRS30006032 / Q01 values by year joined to US population
# where population is available.
#
# Primary:   Spark SQL semantic view
# Alternate: PySpark DataFrame API
# ============================================================

df_q3_primary = (
    spark.table("rearc.gold.v_prs30006032_q01_population")
    .select(
        "series_id",
        "year",
        "period",
        "value",
        "population"
    )
)

df_q3_pyspark = (
    spark.table("rearc.gold.bls_population_analysis")
    .filter(
        (F.col("series_id") == "PRS30006032") &
        (F.col("period") == "Q01")
    )
    .select(
        "series_id",
        "year",
        "period",
        "value",
        "population"
    )
)

print("Q3 - PySpark alternate result")
display(df_q3_pyspark.orderBy("year"))

validate_parity(
    "Q3",
    df_q3_primary,
    df_q3_pyspark
)


# COMMAND ----------
# ============================================================
# Additional validation summary
# ============================================================

print("\nAdditional validation")

q2_series_count = df_q2_primary.select("series_id").distinct().count()

q2_quarter_validation = (
    spark.table("rearc.gold.bls_series_yearly_stats")
    .agg(
        F.min("quarter_count").alias("min_quarters"),
        F.max("quarter_count").alias("max_quarters"),
        F.sum(
            F.when(F.col("quarter_count") > 4, 1).otherwise(0)
        ).alias("invalid_rows")
    )
)

q3_coverage = (
    df_q3_primary
    .agg(
        F.count("*").alias("q3_rows"),
        F.sum(
            F.when(F.col("population").isNotNull(), 1).otherwise(0)
        ).alias("population_matches"),
        F.sum(
            F.when(F.col("population").isNull(), 1).otherwise(0)
        ).alias("population_missing")
    )
)

print(f"Q2 best-year series count: {q2_series_count}")

print("Q2 quarter validation")
display(q2_quarter_validation)

print("Q3 population coverage")
display(q3_coverage)
