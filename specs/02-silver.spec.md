# M02 — Silver: Type / Clean / Conform / Dedupe Spec

| Field | Value |
|-------|-------|
| **Milestone** | M02 |
| **Status** | In Progress 🔨 (notebook built — pending operator run) |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-15 |
| **Completed** | — |
| **Depends on** | M1 (Bronze) |

---

## 1. Objective

Turn the 14 raw Bronze tables into a **trusted, typed, de-duplicated relational model** in
`f1.silver.*` — real NULLs instead of `\N`, real numbers and dates instead of strings, verified
foreign keys — with every rejected row preserved in `f1.quarantine.*` **with a reason**, so that
row accounting closes exactly: `bronze = silver + quarantine`, per table.

## 2. Scope

**In scope:** all 14 tables — cleaning, typing, snake_case conforming, natural-key dedup, row-level
rules, referential checks, per-table quarantine with reasons.

**Out of scope:** joins/denormalization, aggregations, derived business columns (all M3 Gold);
DLT expectations (M4 re-expresses these same rules natively).

## 3. Data contract

### Global conventions (apply to every table)

1. **`\N` → NULL** first, on every column — the source's MySQL-style null marker becomes a real NULL.
2. **Types by convention:** `*Id` columns → `INT` · `milliseconds` → `BIGINT` · `points` → `DOUBLE` ·
   calendar dates → `DATE` · lap/race/pit **time-strings stay `STRING`** (`"1:34:50.616"`, and 764
   pit-stop durations are `mm:ss` format — unparseable as numbers; where a `milliseconds` column
   exists, *it* is the numeric truth).
3. **snake_case renames:** `raceId` → `race_id`, `positionText` → `position_text`, etc. The layer's
   name changes signal the layer's trust level: camelCase = raw Ergast, snake_case = conformed Silver.
4. **A failed cast quarantines the row** (reason `bad_<col>`) — it never silently becomes NULL.
5. Bronze metadata (`_source_file`, `_ingested_at`) is **not** carried into Silver (Bronze holds
   provenance; Silver rows trace back via primary key). Quarantine rows *do* keep it.

### Per-table contract

| Table | Natural key | FKs (→ silver dim) | Required (NOT NULL) | Nullable **by design** (evidence from source scan) |
|-------|-------------|--------------------|---------------------|-----------------------------------------------------|
| `seasons` | `year` | — | year | — |
| `status` | `status_id` | — | status_id, status | — |
| `circuits` | `circuit_id` | — | circuit_id, circuit_ref, name | alt |
| `constructors` | `constructor_id` | — | constructor_id, constructor_ref, name | — |
| `drivers` | `driver_id` | — | driver_id, driver_ref, forename, surname, dob | number (802 `\N`), code (757) |
| `races` | `race_id` | circuit_id→circuits, year→seasons | race_id, year, round, circuit_id, name, date | time (731), all fp/quali/sprint date+time cols |
| `results` | `result_id` | race_id, driver_id, constructor_id, status_id | result_id, the 3 FKs + status_id, position_text, position_order, points, laps | position (10,953 = DNFs!), grid (20), number (6), time/milliseconds (19,326 = lapped/DNF), fastest-lap cols |
| `sprint_results` | `result_id` | race_id, driver_id, constructor_id, status_id | same as results | same pattern (position 15, rank 367, …) |
| `qualifying` | `qualify_id` | race_id, driver_id, constructor_id | qualify_id, the 3 FKs, position | q1 (164), q2 (4,824), q3 (7,216) — eliminated drivers never set a time |
| `lap_times` | (race_id, driver_id, lap) | race_id, driver_id | all of: race_id, driver_id, lap, time, milliseconds | position |
| `pit_stops` | (race_id, driver_id, stop) | race_id, driver_id | race_id, driver_id, stop, lap, time | duration, milliseconds (3 `\N`) |
| `driver_standings` | `driver_standings_id` | race_id, driver_id | id, race_id, driver_id, points, wins | position, position_text |
| `constructor_standings` | `constructor_standings_id` | race_id, constructor_id | id, race_id, constructor_id, points, wins | position, position_text |
| `constructor_results` | `constructor_results_id` | race_id, constructor_id | id, race_id, constructor_id, points | status (12,947 `\N`; only other value is `"D"` = disqualified) |

### Exemplar full contract — `f1.silver.results` (the central fact)

| Column | Type | Nullable | Note |
|--------|------|----------|------|
| result_id | INT | no | PK |
| race_id / driver_id / constructor_id / status_id | INT | no | FKs, must exist in their dims |
| number | INT | yes | car number |
| grid | INT | yes | ≥ 0 when present |
| position | INT | yes | **NULL = did not finish** — a meaning, not an error |
| position_text | STRING | no | "1"…"R" (retired), "D", "W" … |
| position_order | INT | no | ≥ 1, always populated (sort key) |
| points | DOUBLE | no | ≥ 0 — *recorded* points (10 for a 2008 win, 25 today) |
| laps | INT | no | ≥ 0 |
| time | STRING | yes | display string; NULL for lapped/retired |
| milliseconds | BIGINT | yes | numeric truth for race time; ≥ 1 when present |
| fastest_lap / rank | INT | yes | not recorded before 2004 |
| fastest_lap_time | STRING | yes | |
| fastest_lap_speed | DOUBLE | yes | > 0 when present |

## 4. Transformation rules (applied in this order, per table)

1. Read `f1.bronze.<table>`; set every `\N` to NULL.
2. Cast each column per contract → failure = reason `bad_<col>`.
3. Rename to snake_case.
4. Required-column check → reason `missing_<col>`.
5. Domain rules (§5) → reason `invalid_<rule>`.
6. FK check against **silver** dimensions (NULL FKs skip — `missing_` already caught required ones)
   → reason `orphan_<col>`. Checking against *silver* (not bronze) dims means a quarantined
   dimension row automatically cascades to its facts — referential integrity is end-to-end.
7. Natural-key dedup: key appearing >1× with **identical** rows → keep one, extras get reason
   `exact_duplicate`; key appearing >1× with **conflicting** values → we cannot know which is true,
   so **all** copies get reason `key_conflict`.
8. Split: rows with no reasons → `f1.silver.<table>` (typed, renamed); rows with ≥1 reason →
   `f1.quarantine.<table>` (original Bronze string form + `_reasons` + `_quarantined_at`).
   A row can carry multiple comma-joined reasons.

**Processing order (dependencies):** seasons, status, circuits, constructors, drivers → races →
results, sprint_results, qualifying, lap_times, pit_stops, driver_standings, constructor_standings,
constructor_results.

## 5. Data-quality expectations

| Expectation | Rule | Action on violation |
|-------------|------|---------------------|
| `bad_<col>` | every typed column casts cleanly | quarantine |
| `missing_<col>` | required columns NOT NULL | quarantine |
| `orphan_<col>` | FK exists in silver dim (when NOT NULL) | quarantine |
| `exact_duplicate` / `key_conflict` | natural key unique | quarantine (keep 1 identical copy) |
| `invalid_year_range` | seasons/races: year 1950–2030 | quarantine |
| `invalid_lat_lng` | circuits: lat ∈ [-90,90], lng ∈ [-180,180] | quarantine |
| `invalid_dob` | drivers: dob ∈ [1880-01-01, 2010-12-31] | quarantine |
| `invalid_round` | races: round ≥ 1 | quarantine |
| `invalid_points/laps/grid/ms/lap/stop/wins/order` | non-negatives & ≥1s per §3 | quarantine |
| **Scheduled races are VALID** | `silver.races` keeps rows with date > snapshot (13 of them) and NULL session times | **no action — by design** |

### Known dirt & predicted quarantine (from the pre-scan of the actual CSVs)

The rules above were designed against a full local scan of the source. Predictions the operator's
run must reproduce:

| Table | Predicted silver | Predicted quarantine | Why |
|-------|-----------------:|---------------------:|-----|
| `lap_times` | **873,953** | **2,251** | dup keys confined to the **1988 & 1989 Brazilian GPs** (raceId 372, 356 — upstream double-load): 1,707 `exact_duplicate` extras + 272 conflicting pairs (times differ by 1ms) = 544 `key_conflict` |
| `sprint_results` | **566** | **2** | `missing_status_id`: 2026 Miami GP sprint (raceId 1172), status not yet recorded |
| all other 12 tables | = bronze | **0** | scan found zero cast failures, orphans, or domain violations |
| **Total** | **1,000,396** | **2,253** | **sums to 1,002,649 = Bronze exactly** |

## 6. Acceptance criteria

- [ ] 14 silver + 14 quarantine tables exist (quarantine tables may be empty), typed & snake_case.
- [ ] **Row accounting closes 14/14:** bronze = silver + quarantine, per table (verdict table).
- [ ] Quarantine counts match the §5 predictions (2,251 / 2 / zeros) with reasons as predicted.
- [ ] `silver.races` still has all 13 scheduled 2026 races (calendar not falsely quarantined).
- [ ] `silver.lap_times` natural key is unique (count = distinct key count).
- [ ] Spot check: a DNF row shows `position = NULL`, `position_text = 'R'`, typed schema visible.
- [ ] Operator personally executed every step and saw each result (constitution v1.2.0).
- [ ] Code committed; documented for a learner.

## 7. Hands-on run & verification (operator runbook)

| # | You do | You should see |
|---|--------|----------------|
| 1 | Pull latest `main` in the workspace Git folder; open `src/silver/02_silver_clean`; attach serverless compute | Notebook with teaching cells |
| 2 | Run the **demo cells** | The actual duplicate lap rows of the 1988/89 Brazilian GPs, and a DNF row before/after cleaning (`\N` string → typed NULL) |
| 3 | Run the **contracts + engine cells** | Config dict and functions defined (no data written yet) |
| 4 | Run the **processing loop** cell | 14 progress lines in dependency order: dims → races → facts, each with silver/quarantine counts |
| 5 | Run the **accounting verdict** cell | The M2 frontend: per-table `bronze = silver + quarantine` with ✅, quarantine vs predicted ✅, and the assert passing |
| 6 | Run the **quarantine breakdown** cell | Reasons grouped per table: `lap_times → exact_duplicate 1,707 · key_conflict 544`, `sprint_results → missing_status_id 2` |
| 7 | Run the **spot-check cells** | Typed `results` schema; DNF row with NULL position; 13 scheduled 2026 races alive in `silver.races` |
| 8 | **Catalog Explorer → f1 → silver / quarantine** — browse `silver.results` and `quarantine.lap_times` Sample Data | Typed columns vs. original strings + `_reasons` column |
| 9 | Re-run the whole notebook | Identical numbers (idempotent overwrites) |
| 10 | Report the verdict numbers here; Completion section gets filled and committed | Milestone done |

## 8. Constitution compliance

- **I (layering):** reads Bronze only, writes Silver + quarantine only; forward-only. ✅
- **II (contracts first):** this contract (incl. per-table keys/rules) precedes the code. ✅
- **III (quality enforced):** every violation quarantined **with a reason**; accounting closes;
  nothing silently dropped — even exact duplicates land in quarantine, not the void. ✅
- **IV (Delta + UC):** all outputs `f1.silver.*` / `f1.quarantine.*` Delta tables. ✅
- **V (one pipeline):** notebook first (hands-on learning); M4 re-expresses as DLT expectations. ✅
- **VI (free tier):** one serverless session; window functions on 876K rows are trivial scale. ✅
- **Workflow (v1.2.0):** §7 runbook; verdict + breakdown tables as the visible surface. ✅

---

## ✅ Completion  *(fill in when done)*
- **Completed on:** —
- **What was built:** —
- **Acceptance criteria:** —
- **Actual output schema / row counts:** —
- **Quarantine / DQ results:** —
- **Deviations from spec & why:** —
- **Commit(s):** —

## Changelog
| Date | Change |
|------|--------|
| 2026-07-15 | Spec drafted — rules grounded in a full local scan of the 14 CSVs (predictions in §5) |
| 2026-07-15 | Implementation built (`src/silver/02_silver_clean.py`) — contracts dict mirrors §3 exactly |
