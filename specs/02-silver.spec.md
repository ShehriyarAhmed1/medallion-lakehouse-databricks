# M02 — Silver: Clean / Dedupe / Schema Enforcement Spec

| Field | Value |
|-------|-------|
| **Milestone** | M02 |
| **Status** | ✅ Completed |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-08 |
| **Completed** | 2026-07-08 |
| **Depends on** | M1 (Bronze) |

---

## 1. Objective

Turn the raw Bronze trips into a **trusted, validated Silver table**: exact-duplicate trips removed,
invalid rows **quarantined** (not silently dropped), types enforced, and a few **derived columns** added
so Gold aggregations are easy.

## 2. Scope

**In scope:**
- De-duplicate exact-duplicate trips.
- Validate rows; route failures to a **quarantine** table with a reason.
- Enforce the output schema/types.
- Add derived columns: `trip_duration_min`, `pickup_date`, `pickup_hour`, `avg_speed_mph`.

**Out of scope:**
- Business aggregations / metrics → **Gold (M3)**.
- Re-ingesting source data → **Bronze (M1)**.

## 3. Data contract

**Input:** `nyc_taxi.bronze.trips_raw` (from M1).

**Output — `nyc_taxi.silver.trips_clean`** (Delta, Unity Catalog)
| Column | Type | Nullable | Origin | Notes |
|--------|------|----------|--------|-------|
| `tpep_pickup_datetime` | `timestamp` | no | source | |
| `tpep_dropoff_datetime` | `timestamp` | no | source | must be > pickup |
| `trip_distance` | `double` | no | source | > 0 |
| `fare_amount` | `double` | no | source | > 0 |
| `pickup_zip` | `int` | no | source | not null |
| `dropoff_zip` | `int` | no | source | not null |
| `trip_duration_min` | `double` | no | derived | (dropoff − pickup) in minutes |
| `pickup_date` | `date` | no | derived | `to_date(pickup)` — for daily Gold rollups |
| `pickup_hour` | `int` | no | derived | `hour(pickup)` 0–23 — for hourly Gold rollups |
| `avg_speed_mph` | `double` | no | derived | `trip_distance / (trip_duration_min/60)` |
| `_source` | `string` | no | carried | provenance from Bronze |
| `_processed_at` | `timestamp` | no | added | Silver processing time |

**Quarantine — `nyc_taxi.quarantine.trips_invalid`**: the rejected Bronze rows + `_reject_reason`
(string) + `_rejected_at` (timestamp).

- **Write mode:** idempotent `overwrite` (notebook); materialized in the DLT pipeline (M4).

## 4. Transformation / logic rules

1. **Deduplicate** on the 6 business columns (`tpep_pickup_datetime`, `tpep_dropoff_datetime`,
   `trip_distance`, `fare_amount`, `pickup_zip`, `dropoff_zip`). *Why:* the same trip ingested twice
   must not double-count revenue in Gold.
2. **Validate** each row against the rules in §5. Passing rows → Silver; failing rows → quarantine with
   a `_reject_reason`. *Why (Principle III):* bad data is isolated and countable, never silently dropped.
3. **Derive** `trip_duration_min`, `pickup_date`, `pickup_hour`, `avg_speed_mph` on valid rows.
4. **Enforce schema:** write with a fixed schema (no `overwriteSchema`) so Delta rejects any drift.

## 5. Data-quality expectations
*(These become DLT `@dlt.expect` rules in M4. Action here = quarantine + log count.)*
| Expectation | Rule | Action on violation |
|-------------|------|---------------------|
| `valid_fare` | `fare_amount IS NOT NULL AND fare_amount > 0` | quarantine (`bad_fare`) |
| `valid_distance` | `trip_distance IS NOT NULL AND trip_distance > 0` | quarantine (`bad_distance`) |
| `valid_times` | `pickup/dropoff NOT NULL AND dropoff > pickup` | quarantine (`bad_times`) |
| `valid_zips` | `pickup_zip IS NOT NULL AND dropoff_zip IS NOT NULL` | quarantine (`bad_zip`) |
| `sane_speed` *(tracking)* | `avg_speed_mph <= 100` | **warn only** (log; keep row) |

## 6. Acceptance criteria

- [ ] `nyc_taxi.silver.trips_clean` exists (Delta, UC) with the contracted schema; **no nulls** in key columns.
- [ ] Every Silver row satisfies all `valid_*` rules.
- [ ] `nyc_taxi.quarantine.trips_invalid` exists with `_reject_reason`; **reject counts logged** per reason.
- [ ] Accounting closes: `silver_count + quarantine_count == deduped_bronze_count`.
- [ ] Duplicates removed (Silver is distinct on the business key).
- [ ] Derived columns present and correct (spot-check a few rows).
- [ ] Code in `src/silver/`, committed, documented for a learner.

## 7. Deployment notes
- Reads `nyc_taxi.bronze.trips_raw`; creates schemas `nyc_taxi.silver` and `nyc_taxi.quarantine`.
- Runs on serverless; source is small so full `overwrite` is cheap.

## 8. Constitution compliance
- **I. Layering** ✅ Silver = cleaned/validated/typed. **II. Contracts first** ✅ schema above.
- **III. DQ enforced** ✅ quarantine + logged counts, nothing silently dropped.
- **IV. Delta + UC** ✅. **V. Reproducible** ✅ idempotent overwrite.

---

## ✅ Completion
- **Completed on:** 2026-07-08
- **What was built:** `nyc_taxi.silver.trips_clean` + `nyc_taxi.quarantine.trips_invalid` via
  [`src/silver/02_silver_clean.py`](../src/silver/02_silver_clean.py).
- **Acceptance criteria:** ✅ all met (at the data level — see deviation on nullability metadata).
- **Row accounting (bronze → deduped → silver + quarantine):** 21,932 bronze → 21,932 deduped
  (0 exact duplicates) → **21,847 Silver + 85 quarantine = 21,932**. Accounting closes ✅.
- **Reject counts by reason:** `bad_distance` = 75, `bad_fare` = 10 (85 total).
- **Deviations from spec & why:**
  1. 0 duplicates found in the sample data — dedup logic is in place and correct regardless.
  2. Silver column metadata is `nullable = true` (Spark default) even though key values are non-null by
     construction. True `NOT NULL` enforcement is deferred to DLT expectations (M4). Data-level not-null
     is satisfied.
- **Commit(s):** `feat(m02-silver): implement + verify Silver clean/dedupe/quarantine` (this commit).

## Changelog
| Date | Change |
|------|--------|
| 2026-07-08 | Spec drafted |
| 2026-07-08 | Implemented + verified (21,847 silver / 85 quarantine; accounting closes); marked ✅ Completed |
