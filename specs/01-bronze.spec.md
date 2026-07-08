# M01 — Bronze: Raw Ingestion Spec

| Field | Value |
|-------|-------|
| **Milestone** | M01 |
| **Status** | Draft ⬜ |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-08 |
| **Completed** | — |
| **Depends on** | M0 (planning) |

---

## 1. Objective

Ingest the raw NYC taxi trips from `samples.nyctaxi.trips` into an **immutable Bronze Delta table**,
preserving the source data exactly as-is and adding only **ingestion provenance metadata**. Bronze is the
faithful landing zone — no cleaning, no filtering, no type changes.

## 2. Scope

**In scope:**
- Read all rows/columns from `samples.nyctaxi.trips`.
- Add ingestion metadata columns (`_source`, `_ingested_at`).
- Write to a Delta table `nyc_taxi.bronze.trips_raw` under Unity Catalog.
- Create the medallion containers (catalog `nyc_taxi`, schema `bronze`) if absent.

**Out of scope (belongs to later layers):**
- De-duplication, null handling, validity filtering → **Silver (M2)**.
- Aggregations / business metrics → **Gold (M3)**.
- Casting or renaming source columns.

## 3. Data contract

**Input**
| Source | Object | Notes |
|--------|--------|-------|
| Unity Catalog samples | `samples.nyctaxi.trips` | Built-in Delta table, ~Feb 2016 trips |

**Output** — `nyc_taxi.bronze.trips_raw` (Delta, Unity Catalog)
| Column | Type | Nullable | Source | Description |
|--------|------|----------|--------|-------------|
| `tpep_pickup_datetime` | `timestamp` | yes | source | trip start (unchanged) |
| `tpep_dropoff_datetime` | `timestamp` | yes | source | trip end (unchanged) |
| `trip_distance` | `double` | yes | source | miles (unchanged) |
| `fare_amount` | `double` | yes | source | USD (unchanged) |
| `pickup_zip` | `int` | yes | source | pickup ZIP (unchanged) |
| `dropoff_zip` | `int` | yes | source | dropoff ZIP (unchanged) |
| `_source` | `string` | no | added | provenance, literal `samples.nyctaxi.trips` |
| `_ingested_at` | `timestamp` | no | added | ingestion time (`current_timestamp()`) |

- **Write mode:** idempotent. Standalone notebook uses `overwrite` so re-runs don't duplicate; in the DLT
  pipeline (M4) Bronze becomes a **streaming table** (append/incremental). A clean run yields exactly the
  source rows, once.

## 4. Transformation / logic rules

- **No transformation of source data.** All six source columns pass through with identical names & types.
- Add `_source = 'samples.nyctaxi.trips'` and `_ingested_at = current_timestamp()` for lineage/auditing.
- *Why keep Bronze raw?* If something looks wrong downstream, Bronze is the un-touched record we can
  always trace back to. Cleaning here would destroy that audit trail.

## 5. Data-quality expectations

Bronze intentionally does **not drop or reject** rows — that would violate "raw & immutable" (Principle I).
Optional **tracking-only** expectations (warn/log, never drop) may be added in the DLT pipeline for
observability:
| Expectation | Rule | Action |
|-------------|------|--------|
| `track_nonneg_fare` | `fare_amount >= 0` | **warn only** (log count, keep row) |
| `track_pickup_before_dropoff` | `tpep_pickup_datetime <= tpep_dropoff_datetime` | **warn only** |

Real enforcement (drop/quarantine) happens in **Silver (M2)**.

## 6. Acceptance criteria

- [ ] Catalog `nyc_taxi` and schema `nyc_taxi.bronze` exist.
- [ ] Table `nyc_taxi.bronze.trips_raw` exists as **Delta** in Unity Catalog.
- [ ] Output schema = 6 source columns (same names/types) + `_source` (string) + `_ingested_at` (timestamp).
- [ ] `bronze.trips_raw` row count **== source row count** (no rows lost, none de-duplicated at Bronze).
- [ ] `_source` and `_ingested_at` populated on every row.
- [ ] Re-running ingestion is **idempotent** (row count unchanged, no duplication).
- [ ] Code lives in `src/bronze/`, committed, and documented for a learner.

## 7. Deployment notes

- Runs on serverless compute in the Free Edition workspace.
- **Prerequisite:** the `nyc_taxi` catalog + `bronze` schema must exist. The notebook creates them
  idempotently. *If Free Edition restricts `CREATE CATALOG`*, fall back to the pre-provisioned default
  catalog (e.g. `workspace`) and adjust the namespace — record the actual namespace in Completion.
- No external network needed (source is a built-in table).

## 8. Constitution compliance

- **I. Medallion layering** ✅ — Bronze stays raw/immutable (provenance columns only).
- **II. Contracts before code** ✅ — schema fixed above before implementation.
- **IV. Delta + Unity Catalog** ✅ — Delta table, `catalog.schema.table` namespace.
- **III. Data quality enforced** — *intentional deviation at Bronze:* no row dropping (preserve raw);
  enforcement deferred to Silver. Justified by Principle I.
- **V. Reproducible** ✅ — idempotent write; clean re-run reproduces identical Bronze.

---

## ✅ Completion  *(fill in when done)*
- **Completed on:** —
- **What was built:** —
- **Acceptance criteria:** —
- **Actual output schema / row counts:** —
- **Actual namespace used:** —
- **Deviations from spec & why:** —
- **Commit(s):** —

## Changelog
| Date | Change |
|------|--------|
| 2026-07-08 | Spec drafted |
