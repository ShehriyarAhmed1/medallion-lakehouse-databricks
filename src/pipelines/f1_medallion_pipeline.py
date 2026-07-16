"""
M04 — F1 Medallion Lakehouse: ONE Lakeflow Declarative Pipeline
===============================================================

Re-expresses the whole verified Bronze → Silver → Gold flow (M1–M3 notebooks under src/)
as a single, reproducible Lakeflow Declarative Pipeline.

Spec: specs/04-dlt-pipeline.spec.md

WHY a pipeline instead of the three notebooks?
  - Constitution V ("one reproducible pipeline"): one Start button rebuilds the entire
    medallion, identically, every time (full refresh proves it).
  - Constitution III: the M2 quality rules become native @dp expectations with pass-rate
    metrics in the pipeline UI — enforced, not hoped for.

HOW to run it (this file is NOT executed directly — the pipeline engine reads it):
  Databricks → Jobs & Pipelines → Create → ETL pipeline, serverless.
    - Source code       : this file (src/pipelines/f1_medallion_pipeline.py)
    - Default catalog   : f1
    - Default schema    : medallion    ← set in pipeline settings, not in code
  Press Start. The engine reads the @dp.* definitions, derives the dependency graph from
  the spark.read.table(...) calls, and materializes everything in the right order.

NAMING: single target schema (f1.medallion) with layer-prefixed dataset names
(bronze_races, silver_races, quarantine_races, gold_*). Datasets reference each other by
short name — the engine resolves them inside the pipeline. The notebook-built tables in
f1.bronze/silver/gold/quarantine are untouched: they remain the verified M1–M3 teaching
prototypes; THIS pipeline is the production path from M4 onward.

FALLBACK (record in the spec if used): if `from pyspark import pipelines as dp` fails on
an older channel, use classic DLT names: `import dlt as dp`, then materialized_view→table,
temporary_view→view (expect_all_or_drop keeps its name).

The layer logic below is copied from the verified notebooks on purpose — M4 changes HOW
it runs (declarative, orchestrated, metered), not WHAT it computes.
"""

import re
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

VOLUME = "/Volumes/f1/landing/ergast_csv"


def snake(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


# ──────────────────────────────────────────────────────────────────────────────
# The contracts — specs/02-silver.spec.md §3 as code (identical to the M2 notebook)
# ──────────────────────────────────────────────────────────────────────────────
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

# verified quarantine counts from the operator's M2 run — the audit asserts them
PREDICTED_QUARANTINE = {name: 0 for name in CONTRACTS} | {"lap_times": 2_251, "sprint_results": 2}


# ──────────────────────────────────────────────────────────────────────────────
# The M2 rule engine (verified) — builds the typed + reasoned intermediate
# ──────────────────────────────────────────────────────────────────────────────
def build_staged(name):
    c = CONTRACTS[name]
    types, key = c["types"], c["key"]
    required, rules, fks = c.get("required", []), c.get("rules", {}), c.get("fks", {})
    src_cols = list(types)
    biz = [snake(s) for s in src_cols]

    df = spark.read.table(f"bronze_{name}")
    df = df.select(  # \N -> real NULL, still strings
        *[F.when(F.col(s) == "\\N", None).otherwise(F.col(s)).alias(s) for s in src_cols],
        "_source_file", "_ingested_at",
    )
    df = df.select(  # originals kept as o_* (quarantine evidence) + typed & renamed columns
        *[F.col(s).alias(f"o_{s}") for s in src_cols],
        "_source_file", "_ingested_at",
        *[F.col(s).cast(types[s]).alias(snake(s)) for s in src_cols],
    )

    reasons = []
    for s in src_cols:
        sn = snake(s)
        if types[s] != "string":
            reasons.append(F.when(F.col(f"o_{s}").isNotNull() & F.col(sn).isNull(), f"bad_{sn}"))
        if sn in required:
            reasons.append(F.when(F.col(f"o_{s}").isNull(), f"missing_{sn}"))
    for rule_name, expr in rules.items():  # null-safe: NOT(NULL) adds no reason
        reasons.append(F.when(~F.expr(expr), f"invalid_{rule_name}"))
    for col_, (dim, dim_pk) in fks.items():  # FK check against SILVER dims -> quarantine cascades
        ref = spark.read.table(f"silver_{dim}").select(F.col(dim_pk).alias(f"__ref_{col_}")).distinct()
        df = df.join(ref, F.col(col_) == F.col(f"__ref_{col_}"), "left")
        reasons.append(F.when(F.col(col_).isNotNull() & F.col(f"__ref_{col_}").isNull(), f"orphan_{col_}"))

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

    return df.withColumn("_reasons", F.array_join(F.filter(F.array(*reasons), lambda x: x.isNotNull()), ","))


# ──────────────────────────────────────────────────────────────────────────────
# 🥉🥈🚧 One factory registers bronze + staged + silver + quarantine per table
# ──────────────────────────────────────────────────────────────────────────────
def register_table(name):
    src_cols = list(CONTRACTS[name]["types"])
    biz = [snake(s) for s in src_cols]

    @dp.materialized_view(
        name=f"bronze_{name}",
        comment=f"Bronze: raw copy of {name}.csv + ingest provenance (M1 contract — all STRING).",
    )
    def bronze():
        return (
            spark.read.option("header", True).option("inferSchema", False)
            .csv(f"{VOLUME}/{name}.csv")
            .withColumn("_source_file", F.lit(f"{name}.csv"))
            .withColumn("_ingested_at", F.current_timestamp())
        )

    @dp.temporary_view(
        name=f"staged_{name}",
        comment=f"Internal: {name} typed + _reasons via the M2 rule engine (not stored).",
    )
    def staged():
        return build_staged(name)

    # The gate: DLT drops rows failing the constraint AND reports the pass-rate in the UI.
    # _reasons must be in the returned columns for the constraint to see it, so silver carries
    # it — constant '' by construction (spec §3.3).
    @dp.materialized_view(
        name=f"silver_{name}",
        comment=f"Silver: trusted {name} (M2 contract; quality enforced by the quality_gate expectation).",
    )
    @dp.expect_all_or_drop({"quality_gate": "_reasons = ''"})
    def silver():
        return spark.read.table(f"staged_{name}").select(*biz, "_reasons")

    # Expectations drop-and-count; Constitution III says nothing vanishes silently — the
    # quarantine dataset is the exact complement, in original string form, with reasons.
    @dp.materialized_view(
        name=f"quarantine_{name}",
        comment=f"Quarantine: {name} rows that failed the gate, with _reasons (original string form).",
    )
    def quarantine():
        return (
            spark.read.table(f"staged_{name}")
            .filter(F.col("_reasons") != "")
            .select(*[F.col(f"o_{s}").alias(s) for s in src_cols],
                    "_source_file", "_ingested_at", "_reasons")
            .withColumn("_quarantined_at", F.current_timestamp())
        )


for _name in CONTRACTS:
    register_table(_name)


# ──────────────────────────────────────────────────────────────────────────────
# 🥇 GOLD — the four M3 marts (identical logic; no ORDER BY — a stored Delta table
# has no guaranteed read order, so ordering belongs to the dashboard query, M6)
# ──────────────────────────────────────────────────────────────────────────────
@dp.temporary_view(comment="Internal: results enriched with season, race, driver and constructor names.")
def enriched_results():
    return (
        spark.read.table("silver_results")
        .join(spark.read.table("silver_races")
              .select("race_id", F.col("year").alias("season")), "race_id")
        .join(spark.read.table("silver_drivers")
              .select("driver_id", F.concat_ws(" ", "forename", "surname").alias("driver"),
                      "code", F.col("nationality").alias("driver_nationality")), "driver_id")
        .join(spark.read.table("silver_constructors")
              .select("constructor_id", F.col("name").alias("constructor"),
                      F.col("nationality").alias("constructor_nationality")), "constructor_id")
    )


@dp.materialized_view(comment="Gold: one row per (season, driver) — who dominated each season/era?")
def gold_driver_season_summary():
    return (
        spark.read.table("enriched_results")
        .groupBy("season", "driver_id", "driver", "code", F.col("driver_nationality").alias("nationality"))
        .agg(
            F.count("*").alias("races_entered"),
            F.count(F.when(F.col("position") == 1, 1)).alias("wins"),
            F.count(F.when(F.col("position") <= 3, 1)).alias("podiums"),
            F.count(F.when(F.col("grid") == 1, 1)).alias("poles"),
            F.round(F.sum("points"), 1).alias("points"),
            F.count(F.when(F.col("position").isNull(), 1)).alias("dnfs"),
            F.min("position").alias("best_finish"),
        )
    )


@dp.materialized_view(comment="Gold: one row per (season, constructor) — how have team fortunes shifted?")
def gold_constructor_season_summary():
    return (
        spark.read.table("enriched_results")
        .groupBy("season", "constructor_id", "constructor", F.col("constructor_nationality").alias("nationality"))
        .agg(
            F.count("*").alias("entries"),
            F.count(F.when(F.col("position") == 1, 1)).alias("wins"),
            F.count(F.when(F.col("position") <= 3, 1)).alias("podiums"),
            F.round(F.sum("points"), 1).alias("points"),
            F.min("position").alias("best_finish"),
        )
    )


@dp.materialized_view(comment="Gold: one row per season — how have pit stops changed since 1994?")
def gold_pit_stop_evolution():
    return (
        spark.read.table("silver_pit_stops")
        .join(spark.read.table("silver_races").select("race_id", F.col("year").alias("season")), "race_id")
        .groupBy("season")
        .agg(
            F.count("*").alias("stops"),
            F.round(F.avg("milliseconds") / 1000, 3).alias("avg_stop_s"),
            F.round(F.expr("percentile(milliseconds, 0.5)") / 1000, 3).alias("median_stop_s"),
            F.round(F.min("milliseconds") / 1000, 3).alias("fastest_stop_s"),
        )
    )


@dp.materialized_view(comment="Gold: one row per circuit — what's the history of each track?")
def gold_circuit_stats():
    races = spark.read.table("silver_races")
    winners = (
        spark.read.table("silver_results").filter(F.col("position") == 1)
        .join(races.select("race_id", "circuit_id"), "race_id")
        .groupBy("circuit_id").agg(F.countDistinct("driver_id").alias("distinct_winners"))
    )
    return (
        spark.read.table("silver_circuits")
        .select("circuit_id", F.col("name").alias("circuit"), "location", "country")
        .join(races.groupBy("circuit_id").agg(
            F.count("*").alias("races_held"),
            F.min("year").alias("first_season"),
            F.max("year").alias("last_season")), "circuit_id")
        .join(winners, "circuit_id", "left")
        .fillna(0, ["distinct_winners"])
    )


# ──────────────────────────────────────────────────────────────────────────────
# 🔎 AUDIT — the pipeline proves its own run (spec §5): row accounting per table
# against the operator-verified M1/M2 numbers, and mart reconciliation vs M3
# ──────────────────────────────────────────────────────────────────────────────
@dp.materialized_view(
    comment="M4 verdict 1: bronze = silver + quarantine per table, quarantine == the verified M2 counts."
)
def audit_row_accounting():
    parts = []
    for name in CONTRACTS:
        b = spark.read.table(f"bronze_{name}").agg(F.count("*").alias("bronze"))
        s = spark.read.table(f"silver_{name}").agg(F.count("*").alias("silver"))
        q = spark.read.table(f"quarantine_{name}").agg(F.count("*").alias("quarantine"))
        parts.append(
            b.crossJoin(s).crossJoin(q).select(
                F.lit(name).alias("table"), "bronze", "silver", "quarantine",
                (F.col("bronze") == F.col("silver") + F.col("quarantine")).alias("closes"),
                (F.col("quarantine") == F.lit(PREDICTED_QUARANTINE[name])).alias("as_predicted"),
            )
        )
    return reduce(lambda a, b_: a.unionByName(b_), parts)


@dp.materialized_view(
    comment="M4 verdict 2: gold mart rows + key sums vs the operator-verified M3 golden numbers."
)
def audit_gold_reconciliation():
    def check(mart, sum_col, rows_exp, sum_exp):
        return (
            spark.read.table(f"gold_{mart}")
            .agg(F.count("*").alias("rows"), F.sum(sum_col).alias("key_sum"))
            .select(
                F.lit(mart).alias("mart"), "rows", F.lit(rows_exp).alias("rows_expected"),
                F.col("key_sum").cast("long").alias("key_sum"), F.lit(sum_exp).alias("key_sum_expected"),
                ((F.col("rows") == rows_exp) & (F.col("key_sum") == sum_exp)).alias("ok"),
            )
        )

    return reduce(
        lambda a, b_: a.unionByName(b_),
        [
            check("driver_season_summary", "races_entered", 3_254, 27_436),
            check("constructor_season_summary", "entries", 1_132, 27_436),
            check("pit_stop_evolution", "stops", 33, 22_475),
            check("circuit_stats", "races_held", 78, 1_171),
        ],
    )
