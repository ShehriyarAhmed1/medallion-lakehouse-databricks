"""
M04 — Medallion Lakehouse: ONE Lakeflow Declarative Pipeline
============================================================

Re-expresses the whole Bronze → Silver → Gold flow (prototyped and verified in the
M1–M3 notebooks under src/) as a single, reproducible Lakeflow Declarative Pipeline.

Spec: specs/04-dlt-pipeline.spec.md   ·   Docs: docs/pipeline-flow.md

WHY a pipeline instead of the three notebooks?
  - Constitution V ("one reproducible pipeline"): one run rebuilds the entire medallion,
    identically, every time (full refresh). No hand-ordered, one-off notebook runs.
  - Constitution III ("data quality is enforced"): rules become native @dp expectations
    with pass-rate metrics visible in the pipeline UI — enforced, not hoped for.

HOW to run it (this file is NOT executed directly — DLT runs it; see docs/execution-flow.md):
  Databricks → Jobs & Pipelines → Create pipeline (ETL / Declarative), serverless.
    - Source            : this file (src/pipelines/medallion_pipeline.py)
    - Target catalog    : nyc_taxi
    - Target schema     : medallion   ← set HERE, in pipeline settings, not in code
  Then press Start. DLT reads the @dp.* definitions below, builds the dependency graph
  from the spark.read.table(...) calls, and materializes the 6 datasets in the right order.

NAMING: each dataset's name = its function name. Datasets reference each other by their
short name (e.g. spark.read.table("bronze_trips_raw")); DLT resolves them within the
pipeline. If name resolution ever needs it, qualify as nyc_taxi.medallion.<name>
(recorded in the spec's Completion section if that fallback is used).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# The 6 columns that uniquely identify a trip. The source has no stable row id, so these
# business columns ARE the natural key we deduplicate on (matches the M2 notebook exactly).
BUSINESS_COLS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip",
]


# ──────────────────────────────────────────────────────────────────────────────
# 🥉 BRONZE — copy the source untouched, add only provenance
# ──────────────────────────────────────────────────────────────────────────────
@dp.materialized_view(
    comment="Bronze: raw copy of samples.nyctaxi.trips + ingest provenance. No cleaning — this is the audit trail."
)
def bronze_trips_raw():
    # Bronze rule (Constitution I): keep the source exactly, add only WHERE (_source) and
    # WHEN (_ingested_at) it was ingested. Cleaning here would destroy the ability to prove
    # what the source originally contained.
    return (
        spark.read.table("samples.nyctaxi.trips")
        .withColumn("_source", F.lit("samples.nyctaxi.trips"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ──────────────────────────────────────────────────────────────────────────────
# 🔁 Intermediate — dedupe once, tag each row with its reject reason (temporary view)
# ──────────────────────────────────────────────────────────────────────────────
# Both silver_trips_clean and silver_quarantine need the SAME deduped-and-flagged rows.
# Computing it once in a temporary view (not written to storage) avoids doing the dedupe
# twice and guarantees the valid/invalid split comes from an identical source — so the
# accounting always closes (valid + quarantined == deduped). A temporary view (not a table)
# because it is an internal intermediate, not a deliverable.
@dp.temporary_view(
    comment="Internal: bronze deduped on the 6 business columns, tagged with _reject_reason (first failing rule wins)."
)
def deduped_flagged():
    # First-failing-rule-wins cascade → every bad row gets exactly one, countable reason.
    # (Same rule order as the M2 notebook, so the numbers reconcile.)
    reject_reason = (
        F.when(F.col("fare_amount").isNull()   | (F.col("fare_amount")   <= 0), "bad_fare")
         .when(F.col("trip_distance").isNull() | (F.col("trip_distance") <= 0), "bad_distance")
         .when(F.col("tpep_pickup_datetime").isNull() | F.col("tpep_dropoff_datetime").isNull()
               | (F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")), "bad_times")
         .when(F.col("pickup_zip").isNull() | F.col("dropoff_zip").isNull(), "bad_zip")
         .otherwise(None)  # None = the row passed every rule → it's valid
    )
    return (
        spark.read.table("bronze_trips_raw")
        .dropDuplicates(BUSINESS_COLS)
        .withColumn("_reject_reason", reject_reason)
    )


# ──────────────────────────────────────────────────────────────────────────────
# 🥈 SILVER — the trusted layer: enforce quality via expectations, then derive columns
# ──────────────────────────────────────────────────────────────────────────────
# The expectations do TWO jobs: they DROP violating rows (bad data never reaches Silver)
# and they REPORT a pass-rate per rule in the pipeline UI. This is the data contract made
# executable (Constitution III). We deliberately do NOT pre-filter valid rows here — we let
# the expectations do the dropping, so the UI shows a meaningful pass-rate for each rule.
@dp.materialized_view(
    comment="Silver: cleaned, de-duplicated, schema-enforced trips (the single source of truth). Bad rows dropped by expectations."
)
@dp.expect_all_or_drop({
    "valid_fare":     "fare_amount IS NOT NULL AND fare_amount > 0",
    "valid_distance": "trip_distance IS NOT NULL AND trip_distance > 0",
    "valid_times":    "tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL "
                      "AND tpep_dropoff_datetime > tpep_pickup_datetime",
    "valid_zips":     "pickup_zip IS NOT NULL AND dropoff_zip IS NOT NULL",
})
def silver_trips_clean():
    return (
        spark.read.table("deduped_flagged")
        # Precompute duration/date/hour/speed so Gold aggregations are trivial groupBys.
        .withColumn(
            "trip_duration_min",
            (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60.0,
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        # Safe division: expectations drop bad rows AFTER this function returns, so a transient
        # duration<=0 row (a `bad_times` reject) would divide by zero. Under Spark's ANSI mode
        # that raises an error, so we guard it. Valid rows always have duration>0, so their
        # value is identical to the M2 notebook; the guarded rows are dropped anyway.
        .withColumn(
            "avg_speed_mph",
            F.when(F.col("trip_duration_min") > 0,
                   F.col("trip_distance") / (F.col("trip_duration_min") / 60.0)),
        )
        .withColumn("_processed_at", F.current_timestamp())
        # This select IS the Silver contract (12 columns). Writing an explicit column list
        # means any upstream schema drift shows up as a clear error, not a silent surprise.
        .select(
            "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount",
            "pickup_zip", "dropoff_zip", "trip_duration_min", "pickup_date", "pickup_hour",
            "avg_speed_mph", "_source", "_processed_at",
        )
    )


# ──────────────────────────────────────────────────────────────────────────────
# 🚧 QUARANTINE — the complementary set: rows that failed, kept WITH their reason
# ──────────────────────────────────────────────────────────────────────────────
# Expectations drop-and-count bad rows, but they don't KEEP them. Constitution III says
# nothing is silently dropped, so we materialize the rejected rows (the exact complement of
# Silver) with their _reject_reason, so a human can inspect what was rejected and why.
@dp.materialized_view(
    comment="Quarantine: rows rejected from Silver, kept with _reject_reason so nothing is silently dropped."
)
def silver_quarantine():
    return (
        spark.read.table("deduped_flagged")
        .filter(F.col("_reject_reason").isNotNull())
        .withColumn("_rejected_at", F.current_timestamp())
    )


# ──────────────────────────────────────────────────────────────────────────────
# 🥇 GOLD — business-ready marts, each one groupBy(grain).agg(metrics)
# ──────────────────────────────────────────────────────────────────────────────
# Note: no ORDER BY in these definitions. A Delta table has no guaranteed row order on read,
# so sorting a stored table is a no-op that just adds a shuffle. Ordering ("busiest first",
# "by date") is applied in the SQL dashboard query (M6), where it actually takes effect.

@dp.materialized_view(
    comment="Gold: one row per pickup_date — daily ridership & revenue trend."
)
def gold_daily_metrics():
    return (
        spark.read.table("silver_trips_clean")
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        )
    )


@dp.materialized_view(
    comment="Gold: one row per pickup_hour (0–23) — demand by hour of day."
)
def gold_hourly_demand():
    return (
        spark.read.table("silver_trips_clean")
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        )
    )


@dp.materialized_view(
    comment="Gold: one row per pickup_zip — which pickup areas drive the most business."
)
def gold_top_pickup_zones():
    return (
        spark.read.table("silver_trips_clean")
        .groupBy("pickup_zip")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        )
    )
