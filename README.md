# Rearc Data Quest – Databricks Edition

This repository contains my Databricks implementation of the Rearc Data Quest using:

- Python ingestion
- Unity Catalog Volumes
- Delta Lake
- Spark Declarative Pipelines
- PySpark and Spark SQL
- Unity Catalog security
- Genie
- AI/BI Dashboard

The detailed design decisions, trade-offs, and production improvements are documented in [`PROCESS.md`](PROCESS.md).

---

## Final Answers

| Question | Result | Primary implementation |
|---|---|---|
| Q1 – Mean and standard deviation of annual US population, 2013–2018 | Mean: **322,069,808**; sample stddev: **4,158,441.04** | Spark SQL |
| Q2 – Best year for every BLS series | **237 / 237** quarterly series returned; validation difference count = **0** | PySpark |
| Q3 – `PRS30006032`, `Q01`, joined to population where available | **39** BLS rows; **11** population matches; **28** unmatched years retained | Spark SQL |

Each question was also implemented with the alternate Spark API and independently validated.

---

## Two Important Data Decisions

### `Q05` is not a fifth quarter

BLS uses `Q01`–`Q04` for quarterly observations. `Q05` represents an annual-average period.

For Q2, I therefore aggregate only:

```text
Q01
Q02
Q03
Q04
```

Including `Q05` would mix an annual aggregate into the quarterly sum.

### Q3 uses a LEFT JOIN

The BLS series covers years for which the population API has no value.

A `LEFT JOIN` preserves the BLS observation and returns:

```text
population = NULL
```

when population is unavailable.

This is why the final Q3 result contains all 39 BLS rows rather than only the matched population years.

---

## Architecture

```text
BLS Website                     Data USA API
     |                               |
     +--------- Python ingestion ----+
                     |
                     v
          Immutable raw UC Volume
                     |
                     +----> rearc.audit.ingestion_manifest
                     |
                     v
                  BRONZE
          structured source data
                     |
                     v
                  SILVER
        cleaned and validated data
          |                    |
     current state          SCD Type 2
     reference data         observations
          |                    |
          +---------+----------+
                    |
                    v
                  GOLD
        reusable analytical data
                    |
                    v
             Semantic SQL Views
                    |
             +------+------+
             |             |
           Genie      AI/BI Dashboard
```

---

## Unity Catalog Layout

Catalog:

```text
rearc
```

Schemas:

```text
rearc.bronze
rearc.silver
rearc.gold
rearc.audit
```

Managed raw Volume:

```text
rearc.bronze.raw
```

Source state and idempotency are tracked in:

```text
rearc.audit.ingestion_manifest
```

---

## Ingestion

### BLS

Source:

```text
https://download.bls.gov/pub/time.series/pr/
```

The BLS ingestion notebook:

- discovers files from the directory listing instead of hardcoding filenames,
- uses an identifying `User-Agent`,
- detects new, changed, unchanged, removed, and reactivated source objects,
- calculates SHA-256 hashes,
- writes immutable raw versions,
- updates the ingestion manifest.

Example landing path:

```text
/Volumes/rearc/bronze/raw/bls/inbound/<source_object>/v=<timestamp>/<source_object>
```

### Data USA Population

The population API response is stored as immutable JSON.

The response is serialized consistently and hashed before landing so an unchanged response is not unnecessarily rewritten.

More detail on why the manifest and immutable landing are used is in [`PROCESS.md`](PROCESS.md).

---

## Spark Declarative Pipeline

The pipeline contains:

```text
01_bronze.py
      |
      v
02_silver.py
      |
      v
03_gold.py
```

### Bronze

Bronze exposes structured source-aligned data from the controlled raw landing.

I used materialized views for this assignment because source discovery, change detection, versioning, and idempotency are already handled before Bronze.

For continuously arriving cloud-storage files at larger scale, Auto Loader would be a natural production evolution.

### Silver

Silver performs:

- type conversion,
- normalization,
- deduplication,
- key validation,
- expectations,
- reference-data preparation,
- historical processing where needed.

Most Silver entities are current-state datasets.

BLS observations use SCD Type 2 because BLS values can be revised.

Business key:

```text
series_id + year + period
```

Current observations are identified by:

```text
__END_AT IS NULL
```

SCD2 is maintained with `AUTO CDC FROM SNAPSHOT`.

### Gold

Reusable Gold datasets:

```text
rearc.gold.us_population_yearly
rearc.gold.bls_series_yearly_stats
rearc.gold.bls_series_best_year
rearc.gold.bls_population_analysis
```

Gold contains reusable analytical logic so dashboards and analysts do not need to rebuild joins and aggregations from Silver.

---

## Semantic Views

After the Bronze/Silver/Gold pipeline completes, run:

```text
sql/04_semantic.sql
```

It creates:

```text
rearc.gold.v_us_population_stats_2013_2018
rearc.gold.v_bls_series_best_year
rearc.gold.v_prs30006032_q01_population
```

These are lightweight consumer-facing SQL views over the reusable Gold datasets.

`04_semantic.sql` is intentionally separate from the Declarative Pipeline.

---

## SQL and PySpark Validation

Validation is kept outside the pipeline in:

```text
validation/05_validation_alternates.py
```

Implementation pattern:

```text
Q1  Spark SQL  <-> PySpark
Q2  PySpark     <-> Spark SQL
Q3  Spark SQL  <-> PySpark
```

Final validation:

```text
Q1  PASS
Q2  PASS – difference_count = 0, source series = 237, Gold series = 237
Q3  PASS
```

Validation evidence is available under:

```text
screenshots/validation/
```

---

## Unity Catalog Security

The read-only analytics group is:

```text
rearc-analytics
```

It receives:

```sql
GRANT USE CATALOG ON CATALOG rearc TO `rearc-analytics`;
GRANT USE SCHEMA ON SCHEMA rearc.gold TO `rearc-analytics`;
GRANT SELECT ON SCHEMA rearc.gold TO `rearc-analytics`;
```

The restricted persona was tested successfully:

```text
Gold           -> allowed
Bronze/Silver  -> denied
```

Grant definitions are stored in:

```text
sql/unity_catalog_grants.sql
```

---

## Genie and AI/BI Dashboard

A Genie analytical experience and published AI/BI dashboard were created on top of the Gold/semantic layer.

Dashboard:

```text
Rearc Economic Analytics Dashboard
```

It contains:

- population mean and standard-deviation KPIs,
- BLS best-year detail,
- Top-10 best-year visualization,
- `PRS30006032` Q01 vs US population visualization.

Evidence is available under:

```text
screenshots/genie/
screenshots/dashboard/
```

---

## Repository Layout

```text
rearc-data-quest/
├── ingestion/
│   ├── 01_bls_file_ingestion.py
│   └── 02_population_ingestion.py
├── pipeline/
│   ├── 01_bronze.py
│   ├── 02_silver.py
│   └── 03_gold.py
├── sql/
│   ├── 04_semantic.sql
│   └── unity_catalog_grants.sql
├── validation/
│   └── 05_validation_alternates.py
├── screenshots/
│   ├── ingestion/
│   ├── pipeline/
│   ├── validation/
│   ├── security/
│   ├── genie/
│   └── dashboard/
├── PROCESS.md
└── README.md
```

---

## How to Run

Run the project in this order:

```text
1. Run ingestion/01_bls_file_ingestion.py
2. Run ingestion/02_population_ingestion.py
3. Run the Declarative Pipeline:
      01_bronze.py
      02_silver.py
      03_gold.py
4. Run sql/04_semantic.sql
5. Run validation/05_validation_alternates.py
6. Run sql/unity_catalog_grants.sql if setting up the analytics group
7. Open/test Genie and the AI/BI dashboard
```

---

## Why This Design?

A few key choices:

- **Immutable raw files** preserve source history and support replay.
- **The ingestion manifest** provides idempotency and source-state tracking.
- **Bronze materialized views** keep the assignment simple because ingestion already controls the raw snapshot.
- **Silver SCD2 observations** preserve BLS revisions.
- **Gold datasets** centralize reusable business logic.
- **Semantic views** keep assignment-specific serving logic lightweight.
- **Separate validation** proves SQL and PySpark implementations agree.
- **Gold-only analyst access** keeps the consumer security model simple.

The detailed reasoning behind each choice, including when I would use Auto Loader and how I would make the design metadata-driven at scale, is in [`PROCESS.md`](PROCESS.md).

---

## Production Improvements

If this project grew into an enterprise platform, I would evolve it toward:

- metadata-driven source onboarding,
- reusable HTTP/API/SFTP/JDBC ingestion adapters,
- format-driven CSV/JSON/Parquet/XML parsing,
- Auto Loader for continuously arriving files,
- generic Type 1 / Type 2 processing,
- metadata-driven data-quality rules,
- centralized execution audit and reconciliation,
- CI/CD and environment configuration,
- monitoring and alerting,
- workload-driven Spark/Delta optimization.

These are described in detail in [`PROCESS.md`](PROCESS.md).

---

## AI Usage

AI tools, including ChatGPT and Claude, were used for architecture discussion, documentation research, code review, troubleshooting, and documentation drafting.

Suggestions were reviewed and tested in Databricks. The final implementation and documented results reflect the code that was actually executed and validated.
