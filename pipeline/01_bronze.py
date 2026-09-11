from pyspark import pipelines as dp
from pyspark.sql import functions as F
import re


MANIFEST_TABLE = "rearc.audit.ingestion_manifest"


# ============================================================
# DISCOVER ACTIVE BLS FILES
# ============================================================

def get_active_bls_sources():

    return (
        spark.table(MANIFEST_TABLE)
        .filter(
            (F.col("source_name") == "bls")
            &
            (F.col("status") == "active")
        )
        .select(
            "source_object",
            "volume_path"
        )
        .collect()
    )


# ============================================================
# EXCLUDE NON-TABULAR BLS FILES
# ============================================================

NON_TABULAR_FILES = [
    "pr.contacts",
    "pr.txt"
]


def is_structured_file(
    source_object
):

    return (
        source_object
        not in NON_TABULAR_FILES
    )


# ============================================================
# CREATE SAFE BRONZE TABLE NAME
# ============================================================

def get_bronze_table_name(
    source_object
):

    clean_name = re.sub(
        r"[^0-9a-zA-Z]+",
        "_",
        source_object.lower()
    ).strip("_")

    return (
        f"bls_{clean_name}_raw"
    )


# ============================================================
# CLEAN SOURCE COLUMN NAMES
# ============================================================

def get_clean_column_name(
    column_name
):

    return (
        re.sub(
            r"[^0-9a-zA-Z]+",
            "_",
            column_name.strip().lower()
        )
        .strip("_")
    )


# ============================================================
# CREATE BLS BRONZE MATERIALIZED VIEW
# ============================================================

def define_bronze_mv(
    source_object,
    volume_path
):

    table_name = get_bronze_table_name(
        source_object
    )


    @dp.materialized_view(
        name=table_name,
        comment=f"Latest raw BLS snapshot for {source_object}"
    )
    def bronze_table():

        df = (
            spark.read
            .option(
                "header",
                "true"
            )
            .option(
                "sep",
                "\t"
            )
            .option(
                "inferSchema",
                "false"
            )
            .csv(
                volume_path
            )
        )


        # ----------------------------------------------------
        # Normalize source column names
        # ----------------------------------------------------

        for column_name in df.columns:

            clean_column_name = (
                get_clean_column_name(
                    column_name
                )
            )


            if column_name != clean_column_name:

                df = (
                    df.withColumnRenamed(
                        column_name,
                        clean_column_name
                    )
                )


        return df


# ============================================================
# GENERATE BLS BRONZE MATERIALIZED VIEWS
# ============================================================

for source in get_active_bls_sources():

    source_object = (
        source["source_object"]
    )


    if not is_structured_file(
        source_object
    ):
        continue


    define_bronze_mv(
        source_object=source_object,
        volume_path=source["volume_path"]
    )


# ============================================================
# DATA USA POPULATION - BRONZE
# ============================================================

POPULATION_PATH = (
    "/Volumes/rearc/bronze/raw/population/inbound/"
    "population/v=20260910T175234612825Z/population.json"
)


@dp.materialized_view(
    name="population_raw",
    comment="Raw Data USA population API snapshot"
)
def population_raw():

    raw_df = (
        spark.read
        .option("multiLine", "true")
        .json(POPULATION_PATH)
    )

    return (
        raw_df
        .select(
            F.explode("data").alias("record")
        )
        .select(
            F.col("record.`Nation`")
                .cast("string")
                .alias("nation"),

            F.col("record.`Nation ID`")
                .cast("string")
                .alias("nation_id"),

            F.col("record.`Year`")
                .cast("int")
                .alias("year"),

            F.col("record.`Population`")
                .cast("bigint")
                .alias("population"),

            F.lit("population.json")
                .alias("_source_object"),

            F.lit(POPULATION_PATH)
                .alias("_source_file")
        )
    )