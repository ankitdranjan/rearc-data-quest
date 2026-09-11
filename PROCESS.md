# Rearc Data Quest – Databricks Edition
## PROCESS.md

## 1. What I Built

This project implements the Rearc Data Quest as a small end-to-end Databricks lakehouse.

The goal was not only to answer the three analytical questions. I wanted the solution to show how I would normally separate source ingestion, raw history, data quality, historical tracking, reusable business logic, security, validation, and reporting.

The implemented flow is:

```text
BLS website                     Data USA API
     |                               |
     +--------- Python ingestion ----+
                     |
                     v
          Immutable raw UC Volume
                     |
                     +----> ingestion_manifest
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

The main design principle was simple:

> Keep the assignment easy to understand, but do not build it in a way that would block a future move to a reusable metadata-driven platform.

---

## 2. Unity Catalog Organization

I created one catalog:

```text
rearc
```

with four schemas:

```text
rearc.bronze
rearc.silver
rearc.gold
rearc.audit
```

I also created the managed Volume:

```text
rearc.bronze.raw
```

### Why separate schemas?

Each schema has a different responsibility.

- `bronze` represents source-aligned structured data.
- `silver` contains cleaned and reusable source entities.
- `gold` contains business and analytical datasets.
- `audit` contains ingestion/control information.

This separation also makes security easier. An analyst can receive access to Gold without receiving access to raw files, ingestion metadata, or intermediate transformations.

---

## 3. Source Ingestion

The two sources are different:

- BLS exposes a directory containing multiple files.
- Data USA exposes population data through a REST API.

Because both are HTTP sources, I kept the HTTP interaction outside the Spark Declarative Pipeline.

```text
Remote source
     |
Python requests / source discovery
     |
Immutable raw file
     |
Bronze pipeline
```

### Why keep ingestion separate from the pipeline?

The first responsibility is to capture what the provider sent.

If Silver or Gold fails later, I still have the original raw response and can replay the transformation without calling the provider again.

It also keeps responsibilities clear: Python handles HTTP behavior and landing; the Declarative Pipeline handles data transformation.

### BLS ingestion

The BLS source is:

```text
https://download.bls.gov/pub/time.series/pr/
```

The notebook reads the directory listing and discovers the available files instead of keeping a hardcoded filename list.

This matters because if BLS adds or removes a file, discovery can detect that change.

BLS also requires an identifying `User-Agent`, which is supplied by the HTTP session.

Files are landed using immutable versioned paths such as:

```text
/Volumes/rearc/bronze/raw/bls/inbound/<source_object>/v=<timestamp>/<source_object>
```

If a source file changes, I create another version instead of overwriting the previous one.

### Why immutable paths?

Overwriting raw data makes it difficult to answer questions such as:

- What did the source look like yesterday?
- Which source version produced this result?
- Can I replay an earlier load?

Immutable landing keeps that history and makes troubleshooting much easier.

Non-tabular BLS files such as documentation/contact files are also preserved in the raw Volume because the requirement is to source the full directory, even though those files do not need to become analytical tables.

### Population ingestion

Population is retrieved from the Data USA API and stored as JSON.

The ingestion process:

```text
call API
   |
serialize response consistently
   |
calculate SHA-256
   |
compare with manifest
   |
write only when required
```

The downstream business key is:

```text
nation + year
```

Using the same immutable-landing idea for both sources gives the project one consistent raw-data boundary even though the source formats are different.

---

## 4. Why I Created `ingestion_manifest`

The central ingestion table is:

```text
rearc.audit.ingestion_manifest
```

This is more than an execution log. It stores the current known state of each source object.

Important fields include:

```text
source_name
source_object
source_url
source_modified_dt
source_size
content_hash
landing_dir
volume_path
status
first_seen_dt
last_seen_dt
ingested_dt
```

### What problem does it solve?

Without a manifest, the simplest solution would download and process every file on every run.

The manifest lets ingestion identify:

```text
new
changed
unchanged
removed
reactivated
```

Only the persistent source state needs to remain simple:

```text
active
removed
```

### Why use a content hash?

A modified timestamp or file size is useful, but neither proves that the actual content changed.

SHA-256 gives a stronger comparison. If metadata changes but the bytes are the same, I do not need another physical raw version.

### Why both `last_seen_dt` and `ingested_dt`?

They answer different questions.

`last_seen_dt`:

> When did I last see this object at the source?

`ingested_dt`:

> When did I last physically ingest a new version?

An unchanged source can therefore update `last_seen_dt` without changing `ingested_dt`.

### Why keep removed objects?

If a BLS file disappears, I mark it `removed`; I do not delete its history.

If it later returns, the process can recognize it as reactivated. This also preserves lineage.

---

## 5. Bronze – Why Materialized Views?

Bronze turns the landed files into structured, source-aligned datasets. Transformations are intentionally limited.

For this assignment I used materialized views rather than Auto Loader.

### Why?

By the time Bronze starts, my ingestion layer has already done:

```text
HTTP discovery
     |
change detection
     |
immutable versioning
     |
manifest tracking
     |
controlled raw landing
```

The dataset is also small and batch-oriented.

A materialized view gives me a simple declarative way to parse the controlled raw snapshot and expose it to Silver.

Using Auto Loader here would introduce a second file-discovery/checkpoint mechanism even though the custom ingestion process is already deciding what changed.

### Why not Auto Loader now?

Auto Loader is a strong choice when files continuously arrive in cloud storage and Databricks should incrementally discover them.

For example:

```text
ADLS / S3 / GCS
       |
   Auto Loader
       |
Bronze streaming table
       |
     Silver
```

That is not exactly the source pattern in this assignment. Here the original sources are an HTTP directory and an API.

So the choice was not "MV is better than Auto Loader." The choice was:

> Materialized Views are simpler for this small controlled snapshot design; Auto Loader becomes more valuable when storage itself is the continuous ingestion boundary.

### Known improvement

The current BLS Bronze code resolves active source information while defining the pipeline, and the population Bronze code uses the validated immutable snapshot used for the assignment.

For production I would move source resolution into deployment/configuration so the Declarative Pipeline graph stays static and does not require a source path to be changed in transformation code.

---

## 6. Silver – Where Data Becomes Reusable

Silver is where I apply data-quality and business preparation.

It handles:

- trimming and normalization,
- data types,
- null handling,
- deduplication,
- required-key validation,
- reference mappings,
- expectations,
- historical tracking where useful.

Published Silver entities include:

```text
period
footnote
sector
measure
class
duration
seasonal
series
population
observations
```

### Why are most Silver objects current-state?

Not every table needs history.

Reference mappings such as period, sector, class, or measure are mainly needed to describe the current BLS data. Keeping every historical version would add rows and downstream complexity without helping the assignment.

Population is also consumed as a current annual dataset.

### Why are observations SCD Type 2?

BLS observation values can be revised.

The observation business key is:

```text
series_id + year + period
```

If a value changes from `1.5` to `1.7`, overwriting it would lose the fact that the platform previously received `1.5`.

SCD2 keeps both:

```text
series_id  year  period  value   __START_AT   __END_AT
A          2024  Q01     1.5     ...          ...
A          2024  Q01     1.7     ...          NULL
```

The current row is:

```text
__END_AT IS NULL
```

This gives me current analytics while still preserving revision history.

### Why `AUTO CDC FROM SNAPSHOT`?

BLS does not provide a database-style CDC stream. We receive snapshots/current source state.

I therefore used:

```python
dp.create_auto_cdc_from_snapshot_flow(
    target="rearc.silver.observations",
    source="v_observations_prepared",
    keys=["series_id", "year", "period"],
    stored_as_scd_type=2,
    track_history_column_list=["value", "footnote_codes"]
)
```

Databricks maintains the SCD2 history for me, so I do not need to build and maintain a custom SCD2 `MERGE`.

### Why expectations in Silver?

Bronze should stay close to the source. Silver is the boundary where I expect trustworthy reusable data.

Examples include:

- non-null business keys,
- valid year ranges,
- expected period format,
- positive population.

The intention is to detect bad data before it becomes business output.

---

## 7. CSV/TSV vs JSON – Why the Code Is Separate Today

BLS data is mainly tabular text/TSV. Population is nested JSON.

They require different parsing:

```text
BLS       -> delimiter/header/tabular parsing
Population -> JSON structure/explode/field selection
```

Trying to pretend they are physically identical would make the code harder to understand.

For two source families, I preferred explicit parsing over building a large framework only for the sake of being generic.

### What happens if a new format arrives?

Today, a new physical format such as Parquet or XML would require a small parser change.

That is the main extensibility limitation of the current implementation.

The important part is that the change remains in ingestion/parsing. Gold business logic should not care whether the original bytes came from CSV, JSON, or Parquet.

---

## 8. Gold – Why Both Datasets and Views?

Gold contains reusable analytical/business logic.

Implemented datasets are:

```text
rearc.gold.us_population_yearly
rearc.gold.bls_series_yearly_stats
rearc.gold.bls_series_best_year
rearc.gold.bls_population_analysis
```

### Why not answer the assignment directly from Silver?

Because some logic is reusable.

For example, `bls_series_yearly_stats` performs the annual aggregation of quarterly BLS values. A future dashboard should reuse that calculation instead of rebuilding it.

Similarly, `bls_population_analysis` provides the reusable BLS/population relationship before applying the assignment-specific series filter.

### Why add normal SQL views on top?

The three questions are narrower than the reusable Gold datasets.

After the pipeline completes, `sql/04_semantic.sql` creates:

```text
rearc.gold.v_us_population_stats_2013_2018
rearc.gold.v_bls_series_best_year
rearc.gold.v_prs30006032_q01_population
```

These are lightweight consumer contracts.

```text
Reusable Gold data
       |
Semantic view
       |
Dashboard / Genie / analyst
```

I used normal views because the heavy/reusable work is already done in Gold. Materializing every small filter or projection would create unnecessary stored objects and refresh responsibility.

`04_semantic.sql` is intentionally executed separately after `03_gold.py`; it is not a source file in the Declarative Pipeline.

---

## 9. Analytical Questions and Validation

The assignment asks for both Spark SQL and PySpark. I used one as the primary implementation and the other as an independent validation rather than publishing duplicate permanent tables.

### Q1 – Population mean and standard deviation

Requirement: annual US population from 2013 through 2018 inclusive.

Validated result:

```text
years             = 2013-2018
number_of_years   = 6
mean_population   = 322,069,808
sample_stddev     = 4,158,441.040908095
```

Primary: Spark SQL  
Alternative: PySpark

I used sample standard deviation explicitly so the interpretation is clear.

### Q2 – Best year for each BLS series

For each `series_id`, I calculate:

```text
SUM(value)
```

by year across:

```text
Q01 Q02 Q03 Q04
```

and select the largest annual value.

`Q05` is excluded because it represents an annual average rather than a fifth quarter. Including it would mix an annual aggregate into the quarterly sum.

If two years have the same sum, the later year wins so the result is deterministic.

A human-readable label is joined from the BLS reference data so a consumer does not need to interpret only a code such as `PRS30006032`.

Some series have partial years. I keep them because the requirement says to sum the available quarters; it does not require four quarters. `quarter_count` makes this visible.

Validation:

```text
source quarterly series = 237
Gold best-year series    = 237
difference_count         = 0
status                   = PASS
```

Primary: PySpark  
Alternative: Spark SQL

For Q2 comparison, numeric values are normalized to a reasonable precision during validation so insignificant floating-point representation differences do not create a false failure.

### Q3 – PRS30006032 / Q01 with population

Requirement:

```text
series_id = PRS30006032
period    = Q01
```

with population joined where available.

I use a LEFT JOIN from BLS to population because BLS contains years for which the population API has no value.

An INNER JOIN would silently remove valid BLS history.

Validated result:

```text
BLS rows                 = 39
population matches       = 11
population unavailable   = 28
```

Primary: Spark SQL  
Alternative: PySpark

### Why keep validation outside the pipeline?

`validation/05_validation_alternates.py` is a normal validation notebook/script, not a pipeline source.

That is intentional. Pipeline expectations test data quality; this notebook tests whether the analytical answers produced by two different implementations agree.

Keeping it separate also allows normal Spark actions such as result counts without putting imperative actions into Declarative Pipeline definitions.

---

## 10. Unity Catalog Security

I created the analytics group:

```text
rearc-analytics
```

Its purpose is simple:

> Analysts should be able to query curated Gold data without direct access to Bronze, Silver, raw files, or audit internals.

The grants are:

```sql
GRANT USE CATALOG ON CATALOG rearc TO `rearc-analytics`;
GRANT USE SCHEMA ON SCHEMA rearc.gold TO `rearc-analytics`;
GRANT SELECT ON SCHEMA rearc.gold TO `rearc-analytics`;
```

I tested the restricted persona:

```text
Gold           -> allowed
Bronze/Silver  -> denied
```

### Why grant at schema level?

It is easier to maintain than granting every Gold object separately. New Gold tables/views can follow the same read-only access model without repeatedly editing grants.

---

## 11. Genie and AI/BI Dashboard

I created the analytics experience on top of Gold rather than Bronze or Silver.

This is important because a business user should ask questions using curated business concepts, not raw source structures.

The published dashboard is:

```text
Rearc Economic Analytics Dashboard
```

It includes:

- mean population and standard-deviation KPIs,
- BLS best-year details,
- Top-10 BLS best-year visualization,
- PRS30006032 Q01 value vs US population.

The Q3 chart focuses on years where population is available for easier visual comparison. The underlying semantic view still preserves BLS years where population is `NULL`.

### Why keep the dashboard thin?

I do not want important business rules hidden inside a visualization.

The dashboard should display results. Gold and the semantic views should define the logic.

That means another consumer, such as Genie or an analyst, can use the same definitions and get consistent answers.

---

## 12. What I Would Improve for Production

The assignment implementation is deliberately understandable. If this became a platform with many sources, I would standardize the repeated mechanics.

### 12.1 Metadata-driven source configuration

Instead of source settings being spread across code, I would store metadata such as:

```text
source_name
source_type
source_url
file_format
delimiter
landing_path
target_dataset
business_keys
history_type
```

Adding a source would then become mostly configuration.

### 12.2 One reusable ingestion framework

Today BLS and Data USA have separate source-specific notebooks.

A production framework could use adapters:

```text
                 Generic ingestion runner
                    /      |       \
                   /       |        \
               HTTP       REST      SFTP/JDBC
                 \          |         /
                  +---- standard result ----+
                           |
                     raw Volume
                           |
                       manifest
```

Source-specific code would only handle what is genuinely different, such as API pagination or directory parsing.

### 12.3 Generic file-format handling

Parsing options could be metadata:

```text
source   format   delimiter   multiline
BLS      csv      \t          false
DataUSA  json                 true
```

A reusable reader would choose CSV/JSON/Parquet behavior from configuration.

The goal is not one parser for every format. The goal is one ingestion contract with format differences isolated behind it.

### 12.4 Auto Loader when the landing zone becomes continuous

If many files were continuously arriving in cloud storage, I would evolve Bronze to:

```text
Source/API
   |
immutable raw landing
   |
manifest
   |
Auto Loader
   |
Bronze streaming table
   |
Silver
```

The manifest can remain useful for source audit and business state, while Auto Loader handles scalable incremental file discovery and checkpointing.

### 12.5 Generic Type 1 / Type 2 processing

I would make history behavior configurable:

```text
history_type
business_keys
tracked_columns
sequence_column
delete_behavior
```

A reusable helper could apply current-state or SCD2 processing.

This would make a future change from Type 1 to Type 2 mostly configuration instead of requiring a new custom implementation for every table.

### 12.6 Metadata-driven data quality

Common expectations could also be stored as configuration:

```text
dataset | rule_name           | expression
population | positive_value   | population > 0
```

This would provide consistent validation and reporting across many sources.

### 12.7 Execution audit and reconciliation

The ingestion manifest answers:

> What is the current state of each source object?

A separate execution audit should answer:

> What happened during this pipeline run?

At enterprise scale I would capture run ID, start/end time, status, row counts, inserts, updates, deletes, rejected rows, and error details.

I would also reconcile key counts and aggregates between Raw, Bronze, Silver, and Gold.

### 12.8 CI/CD, monitoring, and performance

For production I would add:

- DEV/TEST/PROD deployment configuration,
- source-controlled deployment automation,
- failure and schema-change alerts,
- abnormal row-count monitoring,
- SCD2 change-volume monitoring,
- dashboard refresh monitoring.

I would optimize only after measuring the workload. At larger scale that could include Liquid Clustering, compaction, predicate/column pruning, join strategy, AQE, skew handling, and Spark UI/query-profile analysis.

---

## 13. Target Enterprise Design

The assignment can evolve without changing the main layer responsibilities:

```text
                     SOURCE SYSTEMS
        +--------------+-------------+-------------+
        |              |             |             |
      REST API      HTTP files      SFTP          JDBC
        |              |             |             |
        +--------------+-------------+-------------+
                       |
                       v
              Generic Source Adapters
                       |
                       v
              Immutable Raw UC Volume
                       |
               ingestion_manifest
                       |
                       v
              Format-driven Reader
                       |
                       v
             Bronze / Auto Loader
                       |
                       v
              Reusable Silver Layer
                 |             |
               Type 1        Type 2
                 |             |
                 +------+------+
                        |
                        v
                       Gold
                        |
                 Semantic Views
                        |
                 +------+------+
                 |             |
               Genie        Dashboard
```

The principle I would keep is:

> Standardize technical mechanics, but keep business logic visible enough that another engineer can review and understand it.

---

## 14. Final Validation Summary

The final implementation was validated at several levels.

```text
Ingestion
- BLS discovery and raw landing succeeded.
- Unchanged data did not create unnecessary versions.
- Population hashing/idempotency was tested.

Pipeline
- Bronze completed.
- Silver completed.
- Gold completed.
- Semantic SQL views were created separately.

Q1
- 2013-2018, 6 observations
- mean = 322,069,808
- sample stddev = 4,158,441.040908095
- SQL/PySpark parity = PASS

Q2
- source series = 237
- Gold series = 237
- difference_count = 0
- validation = PASS

Q3
- rows = 39
- population matches = 11
- population missing = 28
- SQL/PySpark parity = PASS

Security
- rearc-analytics can read Gold
- restricted user cannot directly read Bronze/Silver

Presentation
- Genie analytical experience created
- AI/BI dashboard published
```

Screenshots in the repository provide evidence for the major ingestion, pipeline, validation, security, Genie, and dashboard steps.

---

## 15. Repository Structure

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

## 16. Retrospective

The most useful lesson from this project was that "metadata-driven" should not mean "make everything dynamic."

The ingestion manifest is a good metadata-driven component because source discovery and idempotency are repeated platform mechanics.

A generic reader is useful because CSV/JSON/Parquet handling is a repeated platform mechanic.

Generic Type 1/Type 2 helpers are useful because change processing is a repeated platform mechanic.

But business decisions such as:

- what defines a BLS observation,
- why Q05 must not be included in quarterly sums,
- how the best year is selected,
- why Q3 uses a left join,

should remain easy to see in the transformation code.

For this assignment I therefore chose explicit transformations where they improve readability, while documenting where metadata would remove repetition at larger scale.

If I continued the project, my first improvement would be to make source onboarding and file-format handling metadata-driven. My second would be to introduce reusable Type 1/Type 2 processing. After that I would add CI/CD, centralized execution auditing, reconciliation, and operational monitoring.

---

## 17. AI Usage Disclosure

I used AI tools, including ChatGPT and Claude, during the project for architecture discussion, Databricks documentation research, code review, troubleshooting, alternative ideas, and documentation drafting.

I did not treat generated suggestions as automatically correct. The code was run in Databricks, errors were investigated, and several design ideas were changed when actual runtime behavior or the source data showed that another approach was better.

The final implementation is therefore based on the code that was actually executed and validated, not on untested generated examples.
