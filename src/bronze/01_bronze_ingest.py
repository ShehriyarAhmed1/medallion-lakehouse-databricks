# Databricks notebook source
# MAGIC %md
# MAGIC # M01 — Bronze: Raw Ingestion
# MAGIC
# MAGIC Ingest `samples.nyctaxi.trips` into the immutable Bronze Delta table
# MAGIC `nyc_taxi.bronze.trips_raw`, preserving the source exactly and adding only
# MAGIC ingestion provenance (`_source`, `_ingested_at`).
# MAGIC
# MAGIC Spec: `specs/01-bronze.spec.md` · **Bronze = raw & immutable** (no cleaning happens here).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the medallion containers (idempotent)
# MAGIC Unity Catalog namespace = `catalog.schema.table`. One catalog for the project, one schema per layer.

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS nyc_taxi")
spark.sql("CREATE SCHEMA  IF NOT EXISTS nyc_taxi.bronze")
print("✅ Catalog + bronze schema ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the source and add provenance (no cleaning)
# MAGIC Bronze keeps the raw source untouched, plus columns that make any row traceable back to
# MAGIC *where* and *when* it was ingested. Cleaning here would destroy that audit trail.

# COMMAND ----------

from pyspark.sql import functions as F

source = spark.table("samples.nyctaxi.trips")

bronze_df = (
    source
    .withColumn("_source", F.lit("samples.nyctaxi.trips"))  # WHERE it came from
    .withColumn("_ingested_at", F.current_timestamp())      # WHEN we ingested it
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write as a Delta table (idempotent)
# MAGIC `overwrite` keeps this notebook re-runnable — running it repeatedly yields exactly the source
# MAGIC rows once, never duplicates.

# COMMAND ----------

(bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("nyc_taxi.bronze.trips_raw"))

print("✅ Wrote nyc_taxi.bronze.trips_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify against the spec's acceptance criteria

# COMMAND ----------

bronze    = spark.table("nyc_taxi.bronze.trips_raw")
src_count = spark.table("samples.nyctaxi.trips").count()
brz_count = bronze.count()

print(f"Source rows : {src_count:,}")
print(f"Bronze rows : {brz_count:,}")
print(f"Match (no loss / no dup): {src_count == brz_count}")

bronze.printSchema()
display(bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Verified run (2026-07-08):** 21,932 source rows → 21,932 Bronze rows (match). Schema = 6 source
# MAGIC columns + `_source` (string) + `_ingested_at` (timestamp). ✅ Acceptance criteria met.
