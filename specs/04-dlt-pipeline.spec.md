# M04 — DLT: One Lakeflow Declarative Pipeline + Expectations Spec

| Field | Value |
|-------|-------|
| **Milestone** | M04 |
| **Status** | In Progress 🔨 — pipeline code written & documented; **pending workspace run** |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-08 |
| **Completed** | — |
| **Depends on** | M1–M3 (layer logic already prototyped in notebooks) |

---

## 1. Objective

Re-express the entire Bronze → Silver → Gold flow as **one Lakeflow Declarative Pipeline** — the
production, reproducible version of the medallion. Data-quality rules become native **expectations**
(with pass-rate metrics in the pipeline UI), bad rows land in a **quarantine** table, and the whole thing
runs and rebuilds with a single pipeline run. This directly realizes Constitution Principle V ("one
reproducible pipeline") and Principle III (enforced data quality).

## 2. Scope

**In scope:**
- One pipeline source file (`src/pipelines/medallion_pipeline.py`) using `from pyspark import pipelines as dp`.
- Datasets: `bronze_trips_raw`, `silver_trips_clean` (valid), `silver_quarantine` (invalid),
  `gold_daily_metrics`, `gold_hourly_demand`, `gold_top_pickup_zones`.
- Expectations on Silver (the `valid_*` rules) via `@dp.expect_all_or_drop`.
- Create + run the pipeline in the workspace; verify.

**Out of scope:** dashboard (M6), Unity Catalog governance polish (M5). The M1–M3 notebooks remain as the
development/prototype record.

## 3. Target & data contract

- **Publishes to:** catalog `nyc_taxi`, **schema `medallion`** (dedicated; leaves the notebook tables
  intact). All datasets are Delta, materialized views (full recompute each run = reproducible).
- **Datasets** (logic mirrors M1–M3, verified there):

| Dataset | Kind | Derived from | Notes |
|---------|------|--------------|-------|
| `bronze_trips_raw` | MV | `samples.nyctaxi.trips` | + `_source`, `_ingested_at` |
| `silver_trips_clean` | MV + expectations | `bronze_trips_raw` | dedupe, derive cols, drop invalid |
| `silver_quarantine` | MV | `bronze_trips_raw` | the invalid rows + `_reject_reason` |
| `gold_daily_metrics` | MV | `silver_trips_clean` | per `pickup_date` |
| `gold_hourly_demand` | MV | `silver_trips_clean` | per `pickup_hour` |
| `gold_top_pickup_zones` | MV | `silver_trips_clean` | per `pickup_zip` |

## 4. Logic / expectations

- **Dedupe** on the 6 business columns (temporary view feeding both silver + quarantine).
- **Silver expectations** (`@dp.expect_all_or_drop`), which both *enforce* (drop bad rows) and *report*
  pass rates in the pipeline UI:
  ```
  valid_fare      : fare_amount IS NOT NULL AND fare_amount > 0
  valid_distance  : trip_distance IS NOT NULL AND trip_distance > 0
  valid_times     : pickup/dropoff NOT NULL AND dropoff > pickup
  valid_zips      : pickup_zip IS NOT NULL AND dropoff_zip IS NOT NULL
  ```
- **Quarantine** = the complementary set (rows failing any rule) with a `_reject_reason`, so nothing is
  silently dropped (Principle III).
- **Gold** = the three aggregations from M3.

## 5. Acceptance criteria

- [ ] Pipeline is created (Lakeflow, serverless) and **runs green** end-to-end.
- [ ] All 6 datasets materialize under `nyc_taxi.medallion`.
- [ ] Expectations appear in the pipeline UI with pass-rate metrics.
- [ ] `silver_trips_clean` ≈ 21,847 and `silver_quarantine` ≈ 85 (matches the M2 notebook result).
- [ ] The three `gold_*` marts each reconcile (`sum(trips)`) to the `silver_trips_clean` count.
- [ ] Exactly **one** active pipeline (Free Edition: 1 per type).
- [ ] Pipeline source committed to `src/pipelines/`, documented.

## 6. Deployment notes

- Create via **Jobs & Pipelines → Create pipeline (ETL/Declarative)**, serverless, source =
  `src/pipelines/medallion_pipeline.py`, target catalog `nyc_taxi`, target schema `medallion`.
- Trigger manually (deliberate quota use). Full refresh rebuilds everything reproducibly.
- If inter-dataset name resolution needs qualification, fall back to fully-qualified
  `nyc_taxi.medallion.<name>` (recorded in Completion if needed).

## 7. Constitution compliance
- **V. One reproducible pipeline** ✅ (the headline of this milestone).
- **III. DQ enforced** ✅ native expectations + quarantine + visible metrics.
- **I / II / IV** ✅ layering, contracts, Delta+UC all preserved.

---

## ✅ Completion  *(run-result fields pending the workspace run)*
- **Completed on:** — *(fill after a green run)*
- **What was built:** `src/pipelines/medallion_pipeline.py` — one Lakeflow Declarative Pipeline
  (`from pyspark import pipelines as dp`) defining all 6 datasets: `bronze_trips_raw` (MV),
  `deduped_flagged` (temporary view — dedupe + `_reject_reason`), `silver_trips_clean`
  (MV + `@dp.expect_all_or_drop` on the 4 `valid_*` rules), `silver_quarantine` (MV, the
  complementary rejected set), and `gold_daily_metrics` / `gold_hourly_demand` /
  `gold_top_pickup_zones` (MVs). API confirmed against the Databricks LDP Python reference.
- **Acceptance criteria:** ⬜ pending — every criterion in §5 requires running the pipeline in the
  workspace (row counts, pass-rate metrics, "runs green"), which is done by the operator, not in code.
- **Pipeline run result (dataset row counts, expectation pass rates):** — *(expected, from M2: silver ≈
  21,847; quarantine ≈ 85 = 75 `bad_distance` + 10 `bad_fare`; each gold mart sums to silver. Confirm on run.)*
- **Actual dataset-reference syntax used:** — *(code uses short unqualified names via
  `spark.read.table("<name>")`; record here if the `nyc_taxi.medallion.<name>` fallback was needed.)*
- **Deviations from spec & why:**
  1. **Intermediate view named `deduped_flagged`** (spec §4 called it a "temporary view" without a name).
     Avoids a leading-underscore identifier, which would need quoting as a table name.
  2. **Safe-division guard on `avg_speed_mph`** (`F.when(trip_duration_min > 0, …)`). The M2 notebook derived
     columns *after* filtering to valid rows; a pipeline lets expectations drop rows *after* the function
     returns, so a transient `duration = 0` row (a `bad_times` reject) would divide by zero and, under Spark
     ANSI mode, raise. Valid rows are unaffected — identical values to M2.
  3. **No `ORDER BY` in the Gold marts.** A Delta table has no guaranteed read order, so sorting a stored
     table is a no-op that only adds a shuffle. Ordering is applied in the SQL dashboard query (M6).
  4. **Target schema `medallion` is set in pipeline settings, not in code** — that's where LDP takes the
     target catalog/schema, keeping the source portable.
- **Commit(s):** — *(fill after committing)*

## Changelog
| Date | Change |
|------|--------|
| 2026-07-08 | Spec drafted |
| 2026-07-13 | Pipeline implemented (`src/pipelines/medallion_pipeline.py`); deviations recorded; pending workspace run |
