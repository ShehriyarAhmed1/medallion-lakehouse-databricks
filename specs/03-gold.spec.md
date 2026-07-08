# M03 — Gold: Aggregated Business Marts Spec

| Field | Value |
|-------|-------|
| **Milestone** | M03 |
| **Status** | ✅ Completed |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-08 |
| **Completed** | 2026-07-08 |
| **Depends on** | M2 (Silver) |

---

## 1. Objective

Aggregate the trusted `silver.trips_clean` rows into three small, dashboard-ready **Gold marts** that
answer concrete business questions. Gold is consumption-ready: no row-level detail, just metrics.

## 2. Scope

**In scope:** three aggregate tables — `gold.daily_metrics`, `gold.hourly_demand`,
`gold.top_pickup_zones`.
**Out of scope:** cleaning/validation (Silver), visualisation (dashboard, M6).

## 3. Data contract

**Input:** `nyc_taxi.silver.trips_clean` (21,847 rows).
**Schema:** `nyc_taxi.gold` (Delta, Unity Catalog). All marts written idempotently (`overwrite`).

### 3.1 `gold.daily_metrics` — grain: one row per `pickup_date`
| Column | Type | Metric |
|--------|------|--------|
| `pickup_date` | `date` | grouping key |
| `trips` | `bigint` | `count(*)` |
| `total_revenue` | `double` | `sum(fare_amount)` |
| `avg_fare` | `double` | `avg(fare_amount)` |
| `avg_distance` | `double` | `avg(trip_distance)` |
| `avg_duration_min` | `double` | `avg(trip_duration_min)` |

*Answers:* "How do ridership & revenue trend day-to-day?"

### 3.2 `gold.hourly_demand` — grain: one row per `pickup_hour` (0–23)
| Column | Type | Metric |
|--------|------|--------|
| `pickup_hour` | `int` | grouping key |
| `trips` | `bigint` | `count(*)` |
| `avg_fare` | `double` | `avg(fare_amount)` |

*Answers:* "What are the peak hours?"

### 3.3 `gold.top_pickup_zones` — grain: one row per `pickup_zip`
| Column | Type | Metric |
|--------|------|--------|
| `pickup_zip` | `int` | grouping key |
| `trips` | `bigint` | `count(*)` |
| `total_revenue` | `double` | `sum(fare_amount)` |
| `avg_fare` | `double` | `avg(fare_amount)` |

*Answers:* "Which pickup areas drive the most business?" (sorted by `trips` desc)

## 4. Transformation / logic rules

- Each mart = `groupBy(<grain>).agg(...)` over `silver.trips_clean`.
- Round monetary/derived metrics to 2 decimals for readability.
- No filtering — Silver is already validated, so every mart aggregates the full trusted set.

## 5. Data-quality expectations
*(Become DLT `@dlt.expect` rules in M4.)*
| Expectation | Rule | Action |
|-------------|------|--------|
| `positive_trips` | `trips > 0` | drop (an empty group shouldn't exist) |
| `nonneg_revenue` | `total_revenue >= 0` | drop |

## 6. Acceptance criteria

- [ ] `gold.daily_metrics`, `gold.hourly_demand`, `gold.top_pickup_zones` exist (Delta, UC).
- [ ] **Invariant:** `sum(trips)` in *each* mart == Silver row count (21,847) — no rows lost or double-counted.
- [ ] `hourly_demand` has ≤ 24 rows; `pickup_hour` ∈ [0, 23].
- [ ] All metric columns non-null; `total_revenue >= 0`.
- [ ] `top_pickup_zones` sorted by `trips` desc.
- [ ] Code in `src/gold/`, committed, documented.

## 7. Deployment notes
- Reads `nyc_taxi.silver.trips_clean`; creates schema `nyc_taxi.gold`. Serverless; tiny outputs.

## 8. Constitution compliance
- **I. Layering** ✅ Gold = business aggregates. **II. Contracts first** ✅. **IV. Delta + UC** ✅.
- **V. Reproducible** ✅ idempotent overwrite. **III. DQ** ✅ sanity expectations (enforced in M4).

---

## ✅ Completion
- **Completed on:** 2026-07-08
- **What was built:** `gold.daily_metrics`, `gold.hourly_demand`, `gold.top_pickup_zones` via
  [`src/gold/03_gold_marts.py`](../src/gold/03_gold_marts.py).
- **Acceptance criteria:** ✅ all met.
- **Trips-invariant check (each mart vs 21,847):** ✅ all three marts `sum(trips) = 21,847` == Silver.
- **Row counts per mart:** `daily_metrics` = 60 rows (2016-01-01 → 2016-02-29); `hourly_demand` = one row
  per `pickup_hour` (0–23); `top_pickup_zones` = one row per distinct `pickup_zip` (sorted by trips desc).
- **Deviations from spec & why:** none. Note: source data spans Jan–Feb 2016 (60 days), richer than the
  single month originally assumed — better for the daily trend chart (visible Jan-23 blizzard dip).
- **Commit(s):** `feat(m03-gold): implement + verify Gold business marts` (this commit).

## Changelog
| Date | Change |
|------|--------|
| 2026-07-08 | Spec drafted |
| 2026-07-08 | Implemented + verified (all marts reconcile to 21,847); marked ✅ Completed |
