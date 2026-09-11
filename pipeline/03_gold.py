"""
03_gold.py
Rearc Data Quest - Gold reusable business datasets

Gold contains reusable analytical datasets.
Assignment-specific filters and answers are exposed
through semantic views in 04_semantic.sql.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# SILVER SOURCES
# ============================================================

SILVER_POPULATION = "rearc.silver.population"
SILVER_OBSERVATIONS = "rearc.silver.observations"
SILVER_SERIES = "rearc.silver.series"
SILVER_SECTOR = "rearc.silver.sector"
SILVER_MEASURE = "rearc.silver.measure"
SILVER_CLASS = "rearc.silver.class"
SILVER_DURATION = "rearc.silver.duration"
SILVER_SEASONAL = "rearc.silver.seasonal"


# ============================================================
# 1. US POPULATION YEARLY
# Reusable population dataset.
# Exact 2013-2018 statistics will be calculated in semantic SQL.
# ============================================================

@dp.materialized_view(
    name="rearc.gold.us_population_yearly",
    comment="Annual United States population used for analytical reporting."
)
@dp.expect_all_or_drop({
    "year_not_null": "year IS NOT NULL",
    "population_not_null": "population IS NOT NULL"
})
def gold_us_population_yearly():

    return (
        spark.read.table(SILVER_POPULATION)
        .filter(F.lower(F.col("nation")) == "united states")
        .select(
            "nation",
            "nation_id",
            "year",
            "population"
        )
        .dropDuplicates(["nation", "year"])
    )


# ============================================================
# SERIES DESCRIPTION
# Reusable helper for human-readable BLS labels.
# ============================================================

@dp.temporary_view(
    name="v_series_enriched"
)
def series_enriched():

    s = spark.read.table(SILVER_SERIES).alias("s")
    sector = spark.read.table(SILVER_SECTOR).alias("sec")
    measure = spark.read.table(SILVER_MEASURE).alias("mea")
    cls = spark.read.table(SILVER_CLASS).alias("cls")
    duration = spark.read.table(SILVER_DURATION).alias("dur")
    seasonal = spark.read.table(SILVER_SEASONAL).alias("sea")

    return (
        s
        .join(
            sector,
            F.col("s.sector_code") == F.col("sec.sector_code"),
            "left"
        )
        .join(
            measure,
            F.col("s.measure_code") == F.col("mea.measure_code"),
            "left"
        )
        .join(
            cls,
            F.col("s.class_code") == F.col("cls.class_code"),
            "left"
        )
        .join(
            duration,
            F.col("s.duration_code") == F.col("dur.duration_code"),
            "left"
        )
        .join(
            seasonal,
            F.col("s.seasonal_code") == F.col("sea.seasonal_code"),
            "left"
        )
        .select(
            F.col("s.series_id").alias("series_id"),

            F.col("s.sector_code").alias("sector_code"),
            F.col("sec.sector_name").alias("sector_name"),

            F.col("s.measure_code").alias("measure_code"),
            F.col("mea.measure_text").alias("measure_text"),

            F.col("s.class_code").alias("class_code"),
            F.col("cls.class_text").alias("class_text"),

            F.col("s.duration_code").alias("duration_code"),
            F.col("dur.duration_text").alias("duration_text"),

            F.col("s.seasonal_code").alias("seasonal_code"),
            F.col("sea.seasonal_text").alias("seasonal_text"),

            F.concat_ws(
                " | ",
                F.col("sec.sector_name"),
                F.col("mea.measure_text"),
                F.col("cls.class_text")
            ).alias("series_label")
        )
    )


# ============================================================
# 2. BLS SERIES YEARLY STATS
#
# Reusable grain:
# series_id + year
#
# Quarterly values Q01-Q04 are summed.
# Q05 / annual average is deliberately excluded.
# ============================================================

@dp.materialized_view(
    name="rearc.gold.bls_series_yearly_stats",
    comment="Annual BLS series totals calculated from quarterly observations."
)
@dp.expect_all_or_drop({
    "series_id_not_null": "series_id IS NOT NULL",
    "year_not_null": "year IS NOT NULL",
    "annual_value_not_null": "annual_value IS NOT NULL"
})
def gold_bls_series_yearly_stats():

    observations = (
        spark.read.table(SILVER_OBSERVATIONS)
        .filter(F.col("__END_AT").isNull())
        .filter(F.col("period").isin("Q01", "Q02", "Q03", "Q04"))
    )

    yearly = (
        observations
        .groupBy(
            "series_id",
            "year"
        )
        .agg(
            F.sum("value").alias("annual_value"),
            F.countDistinct("period").alias("quarter_count")
        )
    )

    series = spark.read.table("v_series_enriched")

    return (
        yearly.alias("y")
        .join(
            series.alias("s"),
            F.col("y.series_id") == F.col("s.series_id"),
            "left"
        )
        .select(
            F.col("y.series_id"),
            F.col("s.series_label"),
            F.col("s.sector_name"),
            F.col("s.measure_text"),
            F.col("s.class_text"),
            F.col("y.year"),
            F.col("y.annual_value"),
            F.col("y.quarter_count")
        )
    )


# ============================================================
# Q2 PRIMARY IMPLEMENTATION - PYSPARK
#
# Find each series' best year.
# ROW_NUMBER guarantees one row per series.
# If totals tie, latest year wins.
# ============================================================

@dp.materialized_view(
    name="rearc.gold.bls_series_best_year",
    comment="Best year for every BLS series based on highest annual quarterly total."
)
@dp.expect_all_or_drop({
    "series_id_not_null": "series_id IS NOT NULL",
    "best_year_not_null": "best_year IS NOT NULL"
})
def gold_bls_series_best_year():

    yearly = spark.read.table(
        "rearc.gold.bls_series_yearly_stats"
    )

    rank_window = (
        Window
        .partitionBy("series_id")
        .orderBy(
            F.col("annual_value").desc(),
            F.col("year").desc()
        )
    )

    return (
        yearly
        .withColumn(
            "rn",
            F.row_number().over(rank_window)
        )
        .filter(F.col("rn") == 1)
        .select(
            "series_id",
            "series_label",
            F.col("year").alias("best_year"),
            F.col("annual_value").alias("best_year_value"),
            "quarter_count"
        )
    )


# ============================================================
# 3. BLS + POPULATION ANALYSIS
#
# Reusable dataset:
# current quarterly BLS observations joined to annual
# US population by year.
#
# Exact series PRS30006032 + Q01 is filtered in semantic SQL.
# ============================================================

@dp.materialized_view(
    name="rearc.gold.bls_population_analysis",
    comment="Current BLS quarterly observations enriched with US population by year."
)
@dp.expect_all_or_drop({
    "series_id_not_null": "series_id IS NOT NULL",
    "year_not_null": "year IS NOT NULL",
    "period_not_null": "period IS NOT NULL"
})
def gold_bls_population_analysis():

    observations = (
        spark.read.table(SILVER_OBSERVATIONS)
        .filter(F.col("__END_AT").isNull())
        .filter(F.col("period").isin("Q01", "Q02", "Q03", "Q04"))
        .alias("o")
    )

    population = (
        spark.read.table("rearc.gold.us_population_yearly")
        .alias("p")
    )

    return (
        observations
        .join(
            population,
            F.col("o.year") == F.col("p.year"),
            "left"
        )
        .select(
            F.col("o.series_id").alias("series_id"),
            F.col("o.year").alias("year"),
            F.col("o.period").alias("period"),
            F.col("o.value").alias("value"),
            F.col("p.population").alias("population")
        )
    )