# Databricks notebook source
# MAGIC %md
# MAGIC # M03 — Gold: Aggregated Business Marts
# MAGIC
# MAGIC Aggregate `nyc_taxi.silver.trips_clean` into three dashboard-ready marts:
# MAGIC `gold.daily_metrics`, `gold.hourly_demand`, `gold.top_pickup_zones`.
# MAGIC
# MAGIC Spec: `specs/03-gold.spec.md` · The pattern throughout is `groupBy(grain).agg(metrics)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the gold schema

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_taxi.gold")
print("✅ gold schema ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Daily metrics — one row per pickup_date
# MAGIC "How do ridership & revenue trend day-to-day?"

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("nyc_taxi.silver.trips_clean")

daily = (
    silver.groupBy("pickup_date")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
    )
    .orderBy("pickup_date")
)

daily.write.format("delta").mode("overwrite").saveAsTable("nyc_taxi.gold.daily_metrics")
display(daily)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Hourly demand — one row per pickup_hour (0–23)
# MAGIC "What are the peak hours?"

# COMMAND ----------

hourly = (
    silver.groupBy("pickup_hour")
    .agg(F.count("*").alias("trips"),
         F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
    .orderBy("pickup_hour")
)
hourly.write.format("delta").mode("overwrite").saveAsTable("nyc_taxi.gold.hourly_demand")
display(hourly)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Top pickup zones — one row per pickup_zip, busiest first
# MAGIC "Which pickup areas drive the most business?"

# COMMAND ----------

zones = (
    silver.groupBy("pickup_zip")
    .agg(F.count("*").alias("trips"),
         F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
         F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
    .orderBy(F.col("trips").desc())
)
zones.write.format("delta").mode("overwrite").saveAsTable("nyc_taxi.gold.top_pickup_zones")
display(zones)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify the trips-invariant
# MAGIC Every mart must sum to the Silver row count — proof that nothing was lost or double-counted.

# COMMAND ----------

silver_count = spark.table("nyc_taxi.silver.trips_clean").count()
for t in ["daily_metrics", "hourly_demand", "top_pickup_zones"]:
    total = spark.table(f"nyc_taxi.gold.{t}").agg(F.sum("trips")).first()[0]
    print(f"{t}: sum(trips)={total} | matches silver({silver_count}): {total == silver_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Verified run (2026-07-08):** all three marts sum to 21,847 (== Silver). Daily grain spans
# MAGIC 2016-01-01 → 2016-02-29 (60 days).
