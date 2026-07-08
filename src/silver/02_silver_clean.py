# Databricks notebook source
# MAGIC %md
# MAGIC # M02 — Silver: Clean / Dedupe / Schema Enforcement
# MAGIC
# MAGIC Transform `nyc_taxi.bronze.trips_raw` into the trusted `nyc_taxi.silver.trips_clean`:
# MAGIC deduplicate, validate (routing failures to `nyc_taxi.quarantine.trips_invalid`), add derived
# MAGIC columns, and enforce the schema.
# MAGIC
# MAGIC Spec: `specs/02-silver.spec.md` · **Nothing is silently dropped** — bad rows are quarantined with a reason.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the target schemas

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_taxi.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_taxi.quarantine")
print("✅ silver + quarantine schemas ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze and deduplicate
# MAGIC A "trip" is identified by its 6 business columns (not the provenance metadata). Dropping exact
# MAGIC duplicates here prevents double-counting revenue in Gold.

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table("nyc_taxi.bronze.trips_raw")

business_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime",
                 "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip"]

deduped = bronze.dropDuplicates(business_cols)

print("Bronze:", bronze.count(),
      "| Deduped:", deduped.count(),
      "| Duplicates removed:", bronze.count() - deduped.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The quarantine split (the heart of Silver)
# MAGIC Build a single `_reject_reason` column (first failing rule wins; valid rows get `None`), then split
# MAGIC the stream: valid rows -> Silver, invalid rows -> quarantine. Bad data is isolated and countable.

# COMMAND ----------

reject_reason = (
    F.when(F.col("fare_amount").isNull()   | (F.col("fare_amount")   <= 0), "bad_fare")
     .when(F.col("trip_distance").isNull() | (F.col("trip_distance") <= 0), "bad_distance")
     .when(F.col("tpep_pickup_datetime").isNull() | F.col("tpep_dropoff_datetime").isNull()
           | (F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")), "bad_times")
     .when(F.col("pickup_zip").isNull() | F.col("dropoff_zip").isNull(), "bad_zip")
     .otherwise(None)
)

flagged = deduped.withColumn("_reject_reason", reject_reason)

valid   = flagged.filter(F.col("_reject_reason").isNull())      # -> Silver
invalid = flagged.filter(F.col("_reject_reason").isNotNull())   # -> quarantine

print("Valid:", valid.count(), "| Invalid:", invalid.count())
display(invalid.groupBy("_reject_reason").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Derive columns on the valid rows
# MAGIC Precompute duration/date/hour/speed so Gold aggregations are trivial. The final `select` is the
# MAGIC Silver contract.

# COMMAND ----------

silver = (
    valid
    .withColumn("trip_duration_min",
                (F.col("tpep_dropoff_datetime").cast("long")
                 - F.col("tpep_pickup_datetime").cast("long")) / 60.0)
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("avg_speed_mph", F.col("trip_distance") / (F.col("trip_duration_min") / 60.0))
    .withColumn("_processed_at", F.current_timestamp())
    .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount",
            "pickup_zip", "dropoff_zip", "trip_duration_min", "pickup_date", "pickup_hour",
            "avg_speed_mph", "_source", "_processed_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write Silver + quarantine tables
# MAGIC Silver is written WITHOUT `overwriteSchema`, so Delta enforces the schema and rejects any drift.

# COMMAND ----------

(silver.write.format("delta").mode("overwrite")
    .saveAsTable("nyc_taxi.silver.trips_clean"))

quarantine = invalid.withColumn("_rejected_at", F.current_timestamp())
(quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("nyc_taxi.quarantine.trips_invalid"))

print("✅ Wrote silver.trips_clean + quarantine.trips_invalid")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify acceptance criteria

# COMMAND ----------

s = spark.table("nyc_taxi.silver.trips_clean")
q = spark.table("nyc_taxi.quarantine.trips_invalid")

print("Silver:", s.count(), "| Quarantine:", q.count(), "| Deduped bronze:", deduped.count())
print("Accounting closes:", s.count() + q.count() == deduped.count())
s.printSchema()
display(s.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Verified run (2026-07-08):** 21,932 deduped (0 exact dups) → 21,847 Silver + 85 quarantine
# MAGIC (75 `bad_distance` + 10 `bad_fare`). Accounting closes ✅.
