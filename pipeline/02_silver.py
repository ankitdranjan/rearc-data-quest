"""
02_silver.py
Rearc Data Quest - Silver layer

Design:
- Bronze represents the latest landed source snapshot.
- Silver performs cleaning, typing, validation, and data-quality checks.
- Reference/master datasets maintain current state.
- BLS observations use SCD Type 2 with AUTO CDC FROM SNAPSHOT.
- Business joins and analytical logic are deferred to Gold.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# BRONZE SOURCES
# ============================================================

BRONZE_OBSERVATIONS = "rearc.bronze.bls_pr_data_1_alldata_raw"
BRONZE_SERIES = "rearc.bronze.bls_pr_series_raw"
BRONZE_PERIOD = "rearc.bronze.bls_pr_period_raw"
BRONZE_FOOTNOTE = "rearc.bronze.bls_pr_footnote_raw"
BRONZE_SECTOR = "rearc.bronze.bls_pr_sector_raw"
BRONZE_MEASURE = "rearc.bronze.bls_pr_measure_raw"
BRONZE_CLASS = "rearc.bronze.bls_pr_class_raw"
BRONZE_DURATION = "rearc.bronze.bls_pr_duration_raw"
BRONZE_SEASONAL = "rearc.bronze.bls_pr_seasonal_raw"
BRONZE_POPULATION = "rearc.bronze.population_raw"

SILVER_OBSERVATIONS = "rearc.silver.observations"


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def require_columns(df, required, dataset):
    """Fail early when the expected Bronze schema changes."""

    missing = set(required) - set(df.columns)

    if missing:
        raise ValueError(
            f"{dataset}: required column(s) missing: {sorted(missing)}. "
            f"Available columns: {sorted(df.columns)}"
        )

    return df


def clean(column_name):
    """Trim strings and convert empty strings to NULL."""

    trimmed = F.trim(F.col(column_name))

    return F.when(
        trimmed == "",
        F.lit(None).cast("string")
    ).otherwise(trimmed)


def mapping_source(source_table, code_column, text_column):
    """Standard transformation used by small BLS reference datasets."""

    df = require_columns(
        spark.read.table(source_table),
        [code_column, text_column],
        source_table
    )

    return (
        df
        .select(
            clean(code_column).alias(code_column),
            clean(text_column).alias(text_column)
        )
        .dropDuplicates([code_column])
    )


# ============================================================
# PERIOD
# ============================================================

@dp.materialized_view(
    name="rearc.silver.period",
    comment="Curated BLS period reference data."
)
@dp.expect_all_or_drop({
    "period_not_null": "period IS NOT NULL",
    "period_name_not_null": "period_name IS NOT NULL"
})
@dp.expect(
    "period_code_well_formed",
    "period RLIKE '^[A-Z][0-9]{2}$'"
)
def silver_period():

    df = require_columns(
        spark.read.table(BRONZE_PERIOD),
        ["period", "period_abbr", "period_name"],
        BRONZE_PERIOD
    )

    return (
        df
        .select(
            F.upper(clean("period")).alias("period"),
            clean("period_abbr").alias("period_abbr"),
            clean("period_name").alias("period_name")
        )
        .dropDuplicates(["period"])
    )


# ============================================================
# FOOTNOTE
# ============================================================

@dp.materialized_view(
    name="rearc.silver.footnote",
    comment="Curated BLS footnote reference data."
)
@dp.expect_or_drop(
    "footnote_code_not_null",
    "footnote_code IS NOT NULL"
)
def silver_footnote():

    return mapping_source(
        BRONZE_FOOTNOTE,
        "footnote_code",
        "footnote_text"
    )


# ============================================================
# SECTOR
# ============================================================

@dp.materialized_view(
    name="rearc.silver.sector",
    comment="Curated BLS sector reference data."
)
@dp.expect_or_drop(
    "sector_code_not_null",
    "sector_code IS NOT NULL"
)
def silver_sector():

    return mapping_source(
        BRONZE_SECTOR,
        "sector_code",
        "sector_name"
    )


# ============================================================
# MEASURE
# ============================================================

@dp.materialized_view(
    name="rearc.silver.measure",
    comment="Curated BLS measure reference data."
)
@dp.expect_or_drop(
    "measure_code_not_null",
    "measure_code IS NOT NULL"
)
def silver_measure():

    return mapping_source(
        BRONZE_MEASURE,
        "measure_code",
        "measure_text"
    )


# ============================================================
# CLASS
# ============================================================

@dp.materialized_view(
    name="rearc.silver.class",
    comment="Curated BLS class reference data."
)
@dp.expect_or_drop(
    "class_code_not_null",
    "class_code IS NOT NULL"
)
def silver_class():

    return mapping_source(
        BRONZE_CLASS,
        "class_code",
        "class_text"
    )


# ============================================================
# DURATION
# ============================================================

@dp.materialized_view(
    name="rearc.silver.duration",
    comment="Curated BLS duration reference data."
)
@dp.expect_or_drop(
    "duration_code_not_null",
    "duration_code IS NOT NULL"
)
def silver_duration():

    return mapping_source(
        BRONZE_DURATION,
        "duration_code",
        "duration_text"
    )


# ============================================================
# SEASONAL
# ============================================================

@dp.materialized_view(
    name="rearc.silver.seasonal",
    comment="Curated BLS seasonal-adjustment reference data."
)
@dp.expect_or_drop(
    "seasonal_code_not_null",
    "seasonal_code IS NOT NULL"
)
def silver_seasonal():

    return mapping_source(
        BRONZE_SEASONAL,
        "seasonal_code",
        "seasonal_text"
    )


# ============================================================
# SERIES
# ============================================================

@dp.materialized_view(
    name="rearc.silver.series",
    comment="Curated BLS series metadata."
)
@dp.expect_all_or_drop({
    "series_id_not_null": "series_id IS NOT NULL",
    "sector_code_not_null": "sector_code IS NOT NULL",
    "measure_code_not_null": "measure_code IS NOT NULL"
})
@dp.expect(
    "series_id_looks_like_pr",
    "series_id RLIKE '^PR'"
)
def silver_series():

    df = require_columns(
        spark.read.table(BRONZE_SERIES),
        [
            "series_id",
            "sector_code",
            "class_code",
            "measure_code",
            "duration_code",
            "seasonal",
            "base_year",
            "footnote_codes",
            "begin_year",
            "begin_period",
            "end_year",
            "end_period"
        ],
        BRONZE_SERIES
    )

    return (
        df
        .select(
            clean("series_id").alias("series_id"),
            clean("sector_code").alias("sector_code"),
            clean("class_code").alias("class_code"),
            clean("measure_code").alias("measure_code"),
            clean("duration_code").alias("duration_code"),
            clean("seasonal").alias("seasonal_code"),

            F.when(
                clean("base_year") == "-",
                F.lit(None)
            )
            .otherwise(clean("base_year"))
            .cast("int")
            .alias("base_year"),

            clean("footnote_codes").alias("footnote_codes"),

            F.expr(
                "try_cast(trim(begin_year) AS INT)"
            ).alias("begin_year"),

            F.upper(
                clean("begin_period")
            ).alias("begin_period"),

            F.expr(
                "try_cast(trim(end_year) AS INT)"
            ).alias("end_year"),

            F.upper(
                clean("end_period")
            ).alias("end_period")
        )
        .dropDuplicates(["series_id"])
    )


# ============================================================
# OBSERVATIONS - PREPARATION
# ============================================================

@dp.temporary_view(
    name="v_observations_prepared",
    comment="Typed BLS observation snapshot before SCD2 processing."
)
@dp.expect_all_or_drop({
    "series_id_not_null": "series_id IS NOT NULL",
    "year_not_null": "year IS NOT NULL",
    "period_not_null": "period IS NOT NULL",
    "value_is_numeric": "value IS NOT NULL"
})
@dp.expect_all({
    "series_id_looks_like_pr": "series_id RLIKE '^PR'",
    "year_in_plausible_range":
        "year BETWEEN 1900 AND year(current_date()) + 2",
    "period_code_well_formed":
        "period RLIKE '^Q[0-9]{2}$'"
})
def observations_prepared():

    df = require_columns(
        spark.read.table(BRONZE_OBSERVATIONS),
        [
            "series_id",
            "year",
            "period",
            "value",
            "footnote_codes"
        ],
        BRONZE_OBSERVATIONS
    )

    return (
        df
        .select(
            clean("series_id").alias("series_id"),

            F.expr(
                "try_cast(trim(year) AS INT)"
            ).alias("year"),

            F.upper(
                clean("period")
            ).alias("period"),

            F.expr(
                "try_cast(trim(value) AS DOUBLE)"
            ).alias("value"),

            clean("footnote_codes").alias("footnote_codes")
        )
        .dropDuplicates([
            "series_id",
            "year",
            "period"
        ])
    )


# ============================================================
# OBSERVATIONS - SCD TYPE 2 TARGET
# ============================================================

dp.create_streaming_table(
    name=SILVER_OBSERVATIONS,
    comment=(
        "BLS observations maintained as SCD Type 2 "
        "by series_id, year and period."
    ),
    table_properties={
        "quality": "silver"
    }
)


dp.create_auto_cdc_from_snapshot_flow(
    target=SILVER_OBSERVATIONS,
    source="v_observations_prepared",
    keys=[
        "series_id",
        "year",
        "period"
    ],
    stored_as_scd_type=2,
    track_history_column_list=[
        "value",
        "footnote_codes"
    ]
)


# ============================================================
# POPULATION
# Bronze population is already flattened.
# ============================================================

@dp.materialized_view(
    name="rearc.silver.population",
    comment="Curated annual US population data from Data USA."
)
@dp.expect_all_or_drop({
    "nation_not_null": "nation IS NOT NULL",
    "year_not_null": "year IS NOT NULL",
    "population_not_null": "population IS NOT NULL",
    "population_positive": "population > 0"
})
@dp.expect(
    "year_in_plausible_range",
    "year BETWEEN 1900 AND year(current_date()) + 1"
)
def silver_population():

    df = require_columns(
        spark.read.table(BRONZE_POPULATION),
        [
            "nation",
            "nation_id",
            "year",
            "population"
        ],
        BRONZE_POPULATION
    )

    return (
        df
        .select(
            clean("nation").alias("nation"),
            clean("nation_id").alias("nation_id"),
            F.col("year").cast("int").alias("year"),
            F.col("population").cast("bigint").alias("population")
        )
        .dropDuplicates([
            "nation",
            "year"
        ])
    )