# Databricks notebook source
# MAGIC %md
# MAGIC # M2 — Silver: type · clean · conform · dedupe (with quarantine)
# MAGIC
# MAGIC **Spec:** [`specs/02-silver.spec.md`](https://github.com/ShehriyarAhmed1/medallion-lakehouse-databricks/blob/main/specs/02-silver.spec.md)
# MAGIC · run **cell by cell**, reading as you go.
# MAGIC
# MAGIC Bronze preserved the data; Silver **interprets** it. Five moves, applied to every table:
# MAGIC 1. `\N` → real NULL
# MAGIC 2. cast strings to real types (a value that won't cast **quarantines the row** — it never silently becomes NULL)
# MAGIC 3. conform names to snake_case (`raceId` → `race_id`)
# MAGIC 4. de-duplicate on the natural key
# MAGIC 5. validate rows (required fields, domain rules, foreign keys) — failures go to
# MAGIC    `f1.quarantine.<table>` **with a reason**, never into the void
# MAGIC
# MAGIC The gate at the end: **row accounting closes for all 14 tables** — `bronze = silver + quarantine`.
# MAGIC
# MAGIC **We already know what we'll catch.** A local scan of the source CSVs (see spec §5) predicts:
# MAGIC `lap_times` → 2,251 quarantined (duplicate laps from the **1988 & 1989 Brazilian GPs**, loaded twice
# MAGIC upstream) · `sprint_results` → 2 (2026 Miami sprint rows with no status yet) · all other tables clean.
# MAGIC If your run shows anything else, something's wrong — that's what a prediction is for.

# COMMAND ----------

# MAGIC %md
# MAGIC ## See the dirt first — don't take the spec's word for it
# MAGIC
# MAGIC **Exhibit A:** the duplicated laps. One (race, driver, lap) key must be ONE row — here it's two,
# MAGIC differing by a single millisecond. Which is true? Unknowable → both will be quarantined as `key_conflict`.

# COMMAND ----------

from pyspark.sql import functions as F

display(
    spark.table("f1.bronze.lap_times")
    .filter((F.col("raceId") == "372") & (F.col("driverId") == "137") & (F.col("lap").isin("6", "21")))
    .orderBy("lap")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Exhibit B:** a DNF (did-not-finish) row. In Bronze, `position` is the literal 2-character string
# MAGIC `\N` — not a NULL, not a number. Silver must turn it into a real NULL *on purpose*, while
# MAGIC `positionText='R'` (retired) explains why. A missing position here is a **meaning, not an error**.

# COMMAND ----------

display(
    spark.table("f1.bronze.results")
    .filter(F.col("positionText") == "R")
    .select("resultId", "raceId", "driverId", "position", "positionText", "laps", "statusId")
    .limit(3)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The contracts — spec §3 as code
# MAGIC
# MAGIC Per table: column **types** (everything else stays string), the **natural key**, **required**
# MAGIC columns, **domain rules** (SQL, written null-safe), and **foreign keys** checked against the
# MAGIC *silver* dimensions — so a quarantined dimension row would cascade to its facts automatically.
# MAGIC Renames are mechanical: `snake()` converts camelCase, so the convention lives in one function.

# COMMAND ----------

import re
from datetime import datetime, timezone

from pyspark.sql import Window

RUN_TS = datetime.now(timezone.utc)
SNAPSHOT_DATE = "2026-07-05"  # source dump date — races after this are scheduled, not raced


def snake(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


# fmt: off
CONTRACTS = {
    "seasons": {
        "types": {"year": "int", "url": "string"},
        "key": ["year"], "required": ["year"],
        "rules": {"year_range": "year BETWEEN 1950 AND 2030"},
    },
    "status": {
        "types": {"statusId": "int", "status": "string"},
        "key": ["status_id"], "required": ["status_id", "status"],
    },
    "circuits": {
        "types": {"circuitId": "int", "circuitRef": "string", "name": "string", "location": "string",
                  "country": "string", "lat": "double", "lng": "double", "alt": "int", "url": "string"},
        "key": ["circuit_id"], "required": ["circuit_id", "circuit_ref", "name"],
        "rules": {"lat_lng": "lat BETWEEN -90 AND 90 AND lng BETWEEN -180 AND 180"},
    },
    "constructors": {
        "types": {"constructorId": "int", "constructorRef": "string", "name": "string",
                  "nationality": "string", "url": "string"},
        "key": ["constructor_id"], "required": ["constructor_id", "constructor_ref", "name"],
    },
    "drivers": {
        "types": {"driverId": "int", "driverRef": "string", "number": "int", "code": "string",
                  "forename": "string", "surname": "string", "dob": "date", "nationality": "string",
                  "url": "string"},
        "key": ["driver_id"], "required": ["driver_id", "driver_ref", "forename", "surname", "dob"],
        "rules": {"dob_range": "dob BETWEEN DATE'1880-01-01' AND DATE'2010-12-31'"},
    },
    "races": {
        "types": {"raceId": "int", "year": "int", "round": "int", "circuitId": "int", "name": "string",
                  "date": "date", "time": "string", "url": "string",
                  "fp1_date": "date", "fp1_time": "string", "fp2_date": "date", "fp2_time": "string",
                  "fp3_date": "date", "fp3_time": "string", "quali_date": "date", "quali_time": "string",
                  "sprint_date": "date", "sprint_time": "string"},
        "key": ["race_id"], "required": ["race_id", "year", "round", "circuit_id", "name", "date"],
        "rules": {"round_ge_1": "round >= 1", "year_range": "year BETWEEN 1950 AND 2030"},
        "fks": {"circuit_id": ("circuits", "circuit_id"), "year": ("seasons", "year")},
    },
    "results": {
        "types": {"resultId": "int", "raceId": "int", "driverId": "int", "constructorId": "int",
                  "number": "int", "grid": "int", "position": "int", "positionText": "string",
                  "positionOrder": "int", "points": "double", "laps": "int", "time": "string",
                  "milliseconds": "bigint", "fastestLap": "int", "rank": "int",
                  "fastestLapTime": "string", "fastestLapSpeed": "double", "statusId": "int"},
        "key": ["result_id"],
        "required": ["result_id", "race_id", "driver_id", "constructor_id", "status_id",
                     "position_text", "position_order", "points", "laps"],
        "rules": {"points_laps": "points >= 0 AND laps >= 0", "order_ge_1": "position_order >= 1",
                  "grid_ok": "grid IS NULL OR grid >= 0", "ms_ok": "milliseconds IS NULL OR milliseconds >= 1"},
        "fks": {"race_id": ("races", "race_id"), "driver_id": ("drivers", "driver_id"),
                "constructor_id": ("constructors", "constructor_id"), "status_id": ("status", "status_id")},
    },
    "sprint_results": {
        "types": {"resultId": "int", "raceId": "int", "driverId": "int", "constructorId": "int",
                  "number": "int", "grid": "int", "position": "int", "positionText": "string",
                  "positionOrder": "int", "points": "double", "laps": "int", "time": "string",
                  "milliseconds": "bigint", "fastestLap": "int", "fastestLapTime": "string",
                  "statusId": "int", "rank": "int"},
        "key": ["result_id"],
        "required": ["result_id", "race_id", "driver_id", "constructor_id", "status_id",
                     "position_text", "position_order", "points", "laps"],
        "rules": {"points_laps": "points >= 0 AND laps >= 0", "order_ge_1": "position_order >= 1",
                  "grid_ok": "grid IS NULL OR grid >= 0", "ms_ok": "milliseconds IS NULL OR milliseconds >= 1"},
        "fks": {"race_id": ("races", "race_id"), "driver_id": ("drivers", "driver_id"),
                "constructor_id": ("constructors", "constructor_id"), "status_id": ("status", "status_id")},
    },
    "qualifying": {
        "types": {"qualifyId": "int", "raceId": "int", "driverId": "int", "constructorId": "int",
                  "number": "int", "position": "int", "q1": "string", "q2": "string", "q3": "string"},
        "key": ["qualify_id"],
        "required": ["qualify_id", "race_id", "driver_id", "constructor_id", "position"],
        "rules": {"position_ge_1": "position >= 1"},
        "fks": {"race_id": ("races", "race_id"), "driver_id": ("drivers", "driver_id"),
                "constructor_id": ("constructors", "constructor_id")},
    },
    "lap_times": {
        "types": {"raceId": "int", "driverId": "int", "lap": "int", "position": "int",
                  "time": "string", "milliseconds": "bigint"},
        "key": ["race_id", "driver_id", "lap"],
        "required": ["race_id", "driver_id", "lap", "time", "milliseconds"],
        "rules": {"lap_ge_1": "lap >= 1", "ms_ge_1": "milliseconds >= 1"},
        "fks": {"race_id": ("races", "race_id"), "driver_id": ("drivers", "driver_id")},
    },
    "pit_stops": {
        "types": {"raceId": "int", "driverId": "int", "stop": "int", "lap": "int",
                  "time": "string", "duration": "string", "milliseconds": "bigint"},
        "key": ["race_id", "driver_id", "stop"],
        "required": ["race_id", "driver_id", "stop", "lap", "time"],
        "rules": {"stop_lap": "stop >= 1 AND lap >= 1", "ms_ok": "milliseconds IS NULL OR milliseconds >= 1"},
        "fks": {"race_id": ("races", "race_id"), "driver_id": ("drivers", "driver_id")},
    },
    "driver_standings": {
        "types": {"driverStandingsId": "int", "raceId": "int", "driverId": "int", "points": "double",
                  "position": "int", "positionText": "string", "wins": "int"},
        "key": ["driver_standings_id"],
        "required": ["driver_standings_id", "race_id", "driver_id", "points", "wins"],
        "rules": {"points_wins": "points >= 0 AND wins >= 0"},
        "fks": {"race_id": ("races", "race_id"), "driver_id": ("drivers", "driver_id")},
    },
    "constructor_standings": {
        "types": {"constructorStandingsId": "int", "raceId": "int", "constructorId": "int",
                  "points": "double", "position": "int", "positionText": "string", "wins": "int"},
        "key": ["constructor_standings_id"],
        "required": ["constructor_standings_id", "race_id", "constructor_id", "points", "wins"],
        "rules": {"points_wins": "points >= 0 AND wins >= 0"},
        "fks": {"race_id": ("races", "race_id"), "constructor_id": ("constructors", "constructor_id")},
    },
    "constructor_results": {
        "types": {"constructorResultsId": "int", "raceId": "int", "constructorId": "int",
                  "points": "double", "status": "string"},
        "key": ["constructor_results_id"],
        "required": ["constructor_results_id", "race_id", "constructor_id", "points"],
        "rules": {"points_ok": "points >= 0"},
        "fks": {"race_id": ("races", "race_id"), "constructor_id": ("constructors", "constructor_id")},
    },
}
# fmt: on

# dimensions first (facts FK-check against silver dims), then races, then the facts
ORDER = ["seasons", "status", "circuits", "constructors", "drivers", "races",
         "results", "sprint_results", "qualifying", "lap_times", "pit_stops",
         "driver_standings", "constructor_standings", "constructor_results"]

# predictions from the local source scan (spec §5) — the run must reproduce these
PREDICTED_QUARANTINE = {name: 0 for name in ORDER} | {"lap_times": 2_251, "sprint_results": 2}

print(f"{len(CONTRACTS)} contracts loaded · processing order: {' → '.join(ORDER)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The engine — spec §4's eight steps as one function
# MAGIC
# MAGIC Each row collects **reasons** as it fails checks (a row can fail several). No reasons → Silver
# MAGIC (typed, renamed). Any reason → quarantine, in its **original Bronze string form** — because if a
# MAGIC value wouldn't cast, only the string form can hold the evidence.
# MAGIC
# MAGIC Dedup logic (step 7): among rows sharing a natural key — identical copies keep one, extras are
# MAGIC `exact_duplicate`; conflicting copies are **all** `key_conflict` (we can't know which is true).

# COMMAND ----------

def process_table(name):
    c = CONTRACTS[name]
    types, key = c["types"], c["key"]
    required, rules, fks = c.get("required", []), c.get("rules", {}), c.get("fks", {})
    src_cols = list(types)
    biz = [snake(s) for s in src_cols]

    bronze = spark.table(f"f1.bronze.{name}")

    # steps 1-3: \N -> NULL, then typed+renamed columns alongside the original strings (o_*)
    df = bronze.select(
        *[F.when(F.col(s) == "\\N", None).otherwise(F.col(s)).alias(s) for s in src_cols],
        "_source_file", "_ingested_at",
    )
    df = df.select(
        *[F.col(s).alias(f"o_{s}") for s in src_cols],
        "_source_file", "_ingested_at",
        *[F.col(s).cast(types[s]).alias(snake(s)) for s in src_cols],
    )

    reasons = []
    for s in src_cols:
        sn = snake(s)
        if types[s] != "string":  # step 2's guarantee: value present but uncastable -> quarantine
            reasons.append(F.when(F.col(f"o_{s}").isNotNull() & F.col(sn).isNull(), f"bad_{sn}"))
        if sn in required:  # step 4
            reasons.append(F.when(F.col(f"o_{s}").isNull(), f"missing_{sn}"))
    for rule_name, expr in rules.items():  # step 5 (null-safe: NOT(NULL) adds no reason)
        reasons.append(F.when(~F.expr(expr), f"invalid_{rule_name}"))

    # step 6: FK checks against the *silver* dimensions (NULL FKs skip; missing_ caught them)
    for col_, (dim, dim_pk) in fks.items():
        ref = spark.table(f"f1.silver.{dim}").select(F.col(dim_pk).alias(f"__ref_{col_}")).distinct()
        df = df.join(ref, F.col(col_) == F.col(f"__ref_{col_}"), "left")
        reasons.append(F.when(F.col(col_).isNotNull() & F.col(f"__ref_{col_}").isNull(), f"orphan_{col_}"))

    # step 7: natural-key dedup via window functions
    w = Window.partitionBy(*key)
    w_rn = Window.partitionBy(*key).orderBy(*biz)
    df = (
        df.withColumn("__copies", F.count("*").over(w))
        .withColumn("__variants", F.size(F.collect_set(F.struct(*biz)).over(w)))
        .withColumn("__rn", F.row_number().over(w_rn))
    )
    dup = F.col("__copies") > 1
    reasons.append(F.when(dup & (F.col("__variants") == 1) & (F.col("__rn") > 1), "exact_duplicate"))
    reasons.append(F.when(dup & (F.col("__variants") > 1), "key_conflict"))

    df = df.withColumn("_reasons", F.array_join(F.filter(F.array(*reasons), lambda x: x.isNotNull()), ","))

    # step 8: the split
    silver_df = df.filter(F.col("_reasons") == "").select(*biz)
    quarantine_df = df.filter(F.col("_reasons") != "").select(
        *[F.col(f"o_{s}").alias(s) for s in src_cols],
        "_source_file", "_ingested_at", "_reasons",
        F.lit(RUN_TS).alias("_quarantined_at"),
    )

    for target, out in [(f"f1.silver.{name}", silver_df), (f"f1.quarantine.{name}", quarantine_df)]:
        out.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(target)

    return bronze.count(), spark.table(f"f1.silver.{name}").count(), spark.table(f"f1.quarantine.{name}").count()

print("engine ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process all 14 tables (dimensions → races → facts)
# MAGIC
# MAGIC Watch the counts: nearly every table should split `N + 0`; `lap_times` and `sprint_results`
# MAGIC should match the predictions exactly.

# COMMAND ----------

stats = {}
for name in ORDER:
    b, s, q = stats[name] = process_table(name)
    print(f"f1.silver.{name:<24} {s:>9,} rows   quarantine {q:>5,}   (bronze {b:>9,})")

print("\nall 14 tables processed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification — row accounting + prediction check (spec §6)
# MAGIC
# MAGIC Two gates per table: **closes** (bronze = silver + quarantine — nothing vanished, nothing
# MAGIC appeared) and **as_predicted** (quarantine count equals the local scan's prediction).

# COMMAND ----------

verdict = []
for name in ORDER:
    b, s, q = stats[name]
    verdict.append((
        name, b, s, q,
        "✅" if b == s + q else "❌ LEAK",
        PREDICTED_QUARANTINE[name],
        "✅" if q == PREDICTED_QUARANTINE[name] else "❌",
    ))
tb, ts, tq = (sum(x) for x in zip(*[stats[n] for n in ORDER]))
verdict.append(("TOTAL", tb, ts, tq, "✅" if tb == ts + tq else "❌", sum(PREDICTED_QUARANTINE.values()), ""))

display(spark.createDataFrame(
    verdict, "table string, bronze long, silver long, quarantine long, closes string, predicted_q long, as_predicted string"
))

closed = all(b == s + q for b, s, q in stats.values())
predicted = all(stats[n][2] == PREDICTED_QUARANTINE[n] for n in ORDER)
assert closed, "row accounting does NOT close — investigate before proceeding"
assert predicted, "quarantine differs from the source-scan prediction — investigate before proceeding"
print(f"M2 Silver verified: accounting closes 14/14 ✅ · {ts:,} trusted + {tq:,} quarantined = {tb:,} bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why was each row quarantined? (constitution III: always a reason)

# COMMAND ----------

breakdown = []
for name in ORDER:
    q = spark.table(f"f1.quarantine.{name}")
    for r in q.select(F.explode(F.split("_reasons", ",")).alias("reason")).groupBy("reason").count().collect():
        breakdown.append((name, r["reason"], r["count"]))

display(spark.createDataFrame(breakdown, "table string, reason string, rows long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot checks — see the trust with your own eyes
# MAGIC
# MAGIC 1. `silver.results` is *typed* now, and the DNF rows show real NULL positions.
# MAGIC 2. The 13 **scheduled** 2026 races survived — quality rules didn't falsely quarantine the calendar.
# MAGIC 3. `silver.lap_times` natural key is now truly unique.

# COMMAND ----------

spark.table("f1.silver.results").printSchema()
display(  # same DNF check as the Bronze demo — position is a real NULL now
    spark.table("f1.silver.results")
    .filter(F.col("position_text") == "R")
    .select("result_id", "race_id", "driver_id", "position", "position_text", "laps", "status_id")
    .limit(3)
)

# COMMAND ----------

scheduled = spark.table("f1.silver.races").filter(F.col("date") > F.lit(SNAPSHOT_DATE))
print(f"scheduled races still in silver: {scheduled.count()} (expected 13)")
display(scheduled.select("race_id", "year", "round", "name", "date").orderBy("date"))

# COMMAND ----------

lt = spark.table("f1.silver.lap_times")
total, distinct_keys = lt.count(), lt.select("race_id", "driver_id", "lap").distinct().count()
print(f"lap_times rows {total:,} vs distinct (race,driver,lap) keys {distinct_keys:,} -> "
      f"{'UNIQUE ✅' if total == distinct_keys else 'DUPLICATES REMAIN ❌'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## You just proved (with your own run)
# MAGIC
# MAGIC 1. **Nothing vanished:** every one of the 1,002,649 Bronze rows is either trusted in Silver or
# MAGIC    sitting in quarantine with a written reason. The accounting closes to the row.
# MAGIC 2. **The data is now typed and relationally sound:** real NULLs, real numbers/dates, unique keys,
# MAGIC    and every foreign key verified against its dimension.
# MAGIC 3. **Judgment was encoded, not improvised:** DNFs and scheduled 2026 races were *kept* on purpose;
# MAGIC    the 1988/89 Brazilian GP duplicate laps and the 2 incomplete Miami sprint rows were *rejected*
# MAGIC    on purpose — each decision written in the spec before the code ran.
# MAGIC
# MAGIC **Next:** record the verdict numbers in the spec's Completion section, commit, and on to
# MAGIC **M3 Gold** — joining this trusted model into business marts.
