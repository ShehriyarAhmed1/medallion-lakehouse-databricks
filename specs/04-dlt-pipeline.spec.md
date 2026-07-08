# M04 — DLT: One Lakeflow Declarative Pipeline + Expectations Spec

| Field | Value |
|-------|-------|
| **Milestone** | M04 |
| **Status** | Draft ⬜ |
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

## ✅ Completion  *(fill in when done)*
- **Completed on:** —
- **What was built:** —
- **Acceptance criteria:** —
- **Pipeline run result (dataset row counts, expectation pass rates):** —
- **Actual dataset-reference syntax used:** —
- **Deviations from spec & why:** —
- **Commit(s):** —

## Changelog
| Date | Change |
|------|--------|
| 2026-07-08 | Spec drafted |
