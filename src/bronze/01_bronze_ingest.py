# Databricks notebook source
# MAGIC %md
# MAGIC # M1 — Bronze: raw ingestion of the 14 F1 CSVs
# MAGIC
# MAGIC **Spec:** [`specs/01-bronze.spec.md`](https://github.com/ShehriyarAhmed1/medallion-lakehouse-databricks/blob/main/specs/01-bronze.spec.md)
# MAGIC · run this notebook **cell by cell** (not Run-All) and read each markdown cell as you go.
# MAGIC
# MAGIC **What Bronze is:** an exact, immutable copy of each source file as a Delta table.
# MAGIC Everything stays a **string** — even the `\N` null markers stay literal. Bronze's contract is
# MAGIC *preservation, not interpretation*: if a cleaning rule is ever wrong, we fix Silver code and
# MAGIC re-run — the raw evidence here never changes. The only things we add are two metadata columns:
# MAGIC `_source_file` (where a row came from) and `_ingested_at` (when this batch landed).
# MAGIC
# MAGIC **Before you continue:** run the two setup cells below (they're safe to re-run — everything is
# MAGIC `IF NOT EXISTS`), then upload the 14 CSVs into the volume:
# MAGIC **Catalog Explorer → f1 → landing → ergast_csv → Upload** (select all files from `~/Downloads/F1/`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Setup 1/2: the catalog and its five schemas (the medallion layers + landing + quarantine).
# MAGIC -- silver/gold/quarantine stay empty until M2/M3 — created now so the catalog is complete.
# MAGIC CREATE CATALOG IF NOT EXISTS f1;
# MAGIC CREATE SCHEMA IF NOT EXISTS f1.landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS f1.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS f1.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS f1.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS f1.quarantine;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Setup 2/2: the volume — governed *file* storage (tables live in schemas, raw files live in volumes).
# MAGIC CREATE VOLUME IF NOT EXISTS f1.landing.ergast_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration — the ingestion contract
# MAGIC
# MAGIC One entry per file, with the **expected row count verified from the local files** (spec §3).
# MAGIC The whole milestone is driven by this dict: adding a 15th file one day would be a one-line change.

# COMMAND ----------

VOLUME_PATH = "/Volumes/f1/landing/ergast_csv"

# file basename (no .csv) -> expected data rows (excluding header), from specs/01-bronze.spec.md §3
EXPECTED_ROWS = {
    "circuits": 78,
    "constructor_results": 12_964,
    "constructor_standings": 13_730,
    "constructors": 214,
    "driver_standings": 35_559,
    "drivers": 865,
    "lap_times": 876_204,
    "pit_stops": 22_475,
    "qualifying": 11_168,
    "races": 1_171,
    "results": 27_436,
    "seasons": 77,
    "sprint_results": 568,
    "status": 140,
}

print(f"{len(EXPECTED_ROWS)} tables expected · {sum(EXPECTED_ROWS.values()):,} rows total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1 — are all 14 files in the volume?
# MAGIC
# MAGIC Expectation `all_files_present` (spec §5): fail loudly *now* rather than discover a missing
# MAGIC table three milestones later.

# COMMAND ----------

uploaded = {f.name.removesuffix(".csv") for f in dbutils.fs.ls(VOLUME_PATH) if f.name.endswith(".csv")}
missing = sorted(set(EXPECTED_ROWS) - uploaded)
extra = sorted(uploaded - set(EXPECTED_ROWS))

display(
    spark.createDataFrame(
        [(name, "✅ uploaded" if name in uploaded else "❌ MISSING") for name in sorted(EXPECTED_ROWS)],
        "file string, status string",
    )
)
if missing:
    raise Exception(f"Upload these files to {VOLUME_PATH} before continuing: {missing}")
if extra:
    print(f"note — unexpected extra files in the volume (ignored): {extra}")
print("all 14 files present ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learn the pattern once — ingest `circuits` step by step
# MAGIC
# MAGIC Before looping over all 14 files, we do ONE table slowly (the smallest: 78 circuits) so every
# MAGIC part of the pattern is understood. Three micro-steps: **read → add metadata → write**.
# MAGIC
# MAGIC The read options are the whole Bronze philosophy in two lines:
# MAGIC - `header=True` — first line is column names, not data.
# MAGIC - `inferSchema=False` — do NOT let Spark guess types. Everything lands as STRING; typing is a
# MAGIC   *decision* we make (and spec!) in Silver, not an accident of inference. Note we also don't set
# MAGIC   a `nullValue` option — so the source's `\N` markers stay literal strings, preserved as-is.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F

# one timestamp for the whole run — all 14 tables get the same batch marker,
# so "which ingest run produced this row?" has a single, consistent answer
RUN_TS = datetime.now(timezone.utc)

raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{VOLUME_PATH}/circuits.csv")
)

print("step 1 — read: every column is a string, exactly as in the file:")
raw.printSchema()
display(raw.limit(5))

# COMMAND ----------

bronze_df = (
    raw
    .withColumn("_source_file", F.lit("circuits.csv"))
    .withColumn("_ingested_at", F.lit(RUN_TS))
)

print("step 2 — metadata added (the ONLY change Bronze is allowed to make):")
display(bronze_df.limit(5))

# COMMAND ----------

# step 3 — write as a Delta table. mode("overwrite") = snapshot semantics: re-running replaces
# the table with the same data instead of appending duplicates — that's what makes this idempotent.
# overwriteSchema lets a future refreshed snapshot change columns without a manual DROP.
(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("f1.bronze.circuits")
)

count = spark.table("f1.bronze.circuits").count()
print(f"step 3 — written: f1.bronze.circuits · {count} rows (expected {EXPECTED_ROWS['circuits']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now apply the same pattern to the remaining 13 files
# MAGIC
# MAGIC Exactly the three steps you just ran, in a loop driven by the config dict. Watch the per-table
# MAGIC progress lines appear as it runs (`lap_times` with its 876K rows takes the longest).

# COMMAND ----------

for name in sorted(EXPECTED_ROWS):
    if name == "circuits":  # already ingested in the walkthrough above
        continue
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(f"{VOLUME_PATH}/{name}.csv")
        .withColumn("_source_file", F.lit(f"{name}.csv"))
        .withColumn("_ingested_at", F.lit(RUN_TS))
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(f"f1.bronze.{name}")
    )
    print(f"f1.bronze.{name:<24} {spark.table(f'f1.bronze.{name}').count():>9,} rows ✅")

print("\nall 14 bronze tables written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification — the M1 verdict table (spec §5–6)
# MAGIC
# MAGIC The milestone's frontend. For every table:
# MAGIC - **rows_ok** — Bronze count equals the expected source count *exactly* (nothing lost, nothing invented)
# MAGIC - **header_ok** — no row where the first column equals its own column name (the header wasn't
# MAGIC   accidentally ingested as data)
# MAGIC
# MAGIC Acceptance (spec §6): **14/14 ✅**. Then run the whole notebook a second time — every number
# MAGIC must be identical (idempotency), because overwrite replaces instead of appending.

# COMMAND ----------

verdict = []
for name, expected in sorted(EXPECTED_ROWS.items()):
    t = spark.table(f"f1.bronze.{name}")
    actual = t.count()
    first_col = t.columns[0]
    header_leaks = t.filter(F.col(first_col) == first_col).count()
    verdict.append((
        f"f1.bronze.{name}",
        expected,
        actual,
        "✅" if actual == expected else "❌",
        "✅" if header_leaks == 0 else f"❌ {header_leaks}",
    ))

passed = sum(1 for v in verdict if v[3] == "✅" and v[4] == "✅")
verdict.append(("TOTAL", sum(EXPECTED_ROWS.values()), sum(v[2] for v in verdict), "", f"{passed}/14 passed"))

display(
    spark.createDataFrame(
        verdict, "table string, expected long, actual long, rows_ok string, header_ok string"
    )
)

assert passed == 14, f"only {passed}/14 tables passed — investigate before marking M1 done"
print(f"M1 Bronze verified: 14/14 tables ✅ · {sum(EXPECTED_ROWS.values()):,} rows accounted for")

# COMMAND ----------

# MAGIC %md
# MAGIC ## You just proved (with your own run)
# MAGIC
# MAGIC 1. **Completeness** — every Bronze table matches its source count exactly: 1,002,649 rows in, zero lost, zero invented.
# MAGIC 2. **Preservation** — open **Catalog Explorer → f1 → bronze → results → Sample Data**: raw strings,
# MAGIC    literal `\N` values, plus `_source_file` / `_ingested_at` on every row.
# MAGIC 3. **Idempotency** — after your second full run: same 14/14 table, same counts.
# MAGIC
# MAGIC **Next:** record these results in the spec's **Completion** section (`specs/01-bronze.spec.md`),
# MAGIC commit via the Git folder, and M1 is done. M2 (Silver) is where we finally *interpret* this data:
# MAGIC real types, real NULLs, deduplication, and quarantine.
