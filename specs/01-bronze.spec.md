# M01 — Bronze: CSV Upload + Raw Ingestion Spec

| Field | Value |
|-------|-------|
| **Milestone** | M01 |
| **Status** | ✅ Completed |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-15 |
| **Completed** | 2026-07-15 |
| **Depends on** | M0 (planning) |

---

## 1. Objective

Land the 14 Formula 1 CSVs into governed storage and create one **immutable Bronze Delta table per
file** — an exact, typed-as-string copy of the source with only ingestion metadata added — so that
every downstream layer can always be rebuilt from (and audited against) the raw data.

## 2. Scope

**In scope:**
- Creating the Unity Catalog objects: catalog `f1`, schemas `landing / bronze / silver / gold /
  quarantine`, volume `f1.landing.ergast_csv` *(silver/gold/quarantine created now so the catalog
  is complete; they stay empty until M2/M3)*.
- Operator upload of the 14 CSVs into the volume via the workspace UI.
- One ingestion notebook (`src/bronze/01_bronze_ingest`) creating 14 tables `f1.bronze.<name>`.
- A verification section that displays an expected-vs-actual verdict table (the M1 frontend).

**Out of scope:**
- Type casting, `\N` handling, deduplication, renames (all M2 — Bronze preserves the mess **by design**).
- DLT pipeline (M4 re-expresses this logic declaratively).

## 3. Data contract

**Input** — 14 CSVs at `/Volumes/f1/landing/ergast_csv/`, snapshot 2026-07-05, header row, `"`-quoted
strings, `\N` as the source's NULL marker (kept as a literal string in Bronze):

| File | Expected rows (verified locally) |
|------|-----------------------------------|
| `circuits.csv` | 78 |
| `constructor_results.csv` | 12,964 |
| `constructor_standings.csv` | 13,730 |
| `constructors.csv` | 214 |
| `driver_standings.csv` | 35,559 |
| `drivers.csv` | 865 |
| `lap_times.csv` | 876,204 |
| `pit_stops.csv` | 22,475 |
| `qualifying.csv` | 11,168 |
| `races.csv` | 1,171 |
| `results.csv` | 27,436 |
| `seasons.csv` | 77 |
| `sprint_results.csv` | 568 |
| `status.csv` | 140 |
| **Total** | **1,002,649** |

**Output** — one Delta table per file: `f1.bronze.<file_basename>` (e.g. `f1.bronze.lap_times`).

| Column | Type | Nullable | Key | Description |
|--------|------|----------|-----|-------------|
| *(all source columns)* | `STRING` | yes | — | Exactly as in the CSV — original camelCase names (`raceId`, `driverId`, …), no casts, `\N` kept literal |
| `_source_file` | `STRING` | no | — | Source file name, e.g. `results.csv` |
| `_ingested_at` | `TIMESTAMP` | no | — | Timestamp of the ingest run |

- **Write mode:** `overwrite` — each run re-lands the full snapshot, making re-runs **idempotent**
  (same input ⇒ same table, no duplicates). Append semantics would double the data on every re-run.
- **Why everything STRING:** Bronze's contract is *preservation, not interpretation*. If a cast fails
  or a rule is wrong, we fix it in Silver code and re-run — the evidence in Bronze never changes.

## 4. Transformation / logic rules

**None — by design.** The only additions are the two metadata columns. Read options:
`header=true`, `inferSchema=false` (all columns as STRING). The notebook loops over a config list of
the 14 file names (metadata-driven ingestion — adding a 15th file would be a one-line change) and
`display()`s progress per table so every step is visible.

## 5. Data-quality expectations

Bronze does not *filter* anything (that's Silver's job); it *verifies completeness*:

| Expectation | Rule | Action on violation |
|-------------|------|---------------------|
| `all_files_present` | 14 expected files found in the volume | stop and fix upload |
| `rowcount_matches_source` | per-table Bronze count == expected count above | investigate before proceeding (parsing issue) |
| `header_not_ingested` | e.g. no row in `circuits` where `circuitId = 'circuitId'` | fix read options |

## 6. Acceptance criteria

- [x] Catalog `f1` with schemas `landing/bronze/silver/gold/quarantine` and volume `ergast_csv` exist.
- [x] All 14 CSVs visible in the volume in Catalog Explorer.
- [x] 14 tables exist at `f1.bronze.<table>` with all source columns as STRING + the 2 metadata columns.
- [x] The verification cell shows **14/14 ✅** — every table's count equals the expected count above.
- [x] Re-running the notebook changes no counts (idempotency via overwrite snapshot writes; runbook step 7).
- [x] Operator personally executed every step and saw each result (constitution v1.2.0).
- [x] Code committed; documented for a learner.

## 7. Hands-on run & verification (operator runbook)

All steps are performed **by the operator, by hand**, in the Free Edition workspace. Tight session —
run it in one sitting, nothing left idling.

| # | You do | You should see |
|---|--------|----------------|
| 1 | Open `src/bronze/01_bronze_ingest` (via the Git folder) and run its two **setup cells** — `CREATE CATALOG/SCHEMA/VOLUME IF NOT EXISTS` (safe to re-run) | Each statement returns OK; **Catalog Explorer** shows `f1` with 5 schemas and an empty volume under `landing` |
| 2 | **Catalog Explorer → f1 → landing → ergast_csv → Upload** — select all 14 CSVs from `~/Downloads/F1/` | 14 files listed in the volume with sizes (largest: `lap_times.csv` ≈ 25 MB) |
| 3 | Back in the notebook, run the **file-presence check** cell | A 14-row table, every file "✅ uploaded" |
| 4 | **Run the rest cell by cell** (not Run-All) — first the `circuits` walkthrough (read → metadata → write, with a `display()` at each micro-step), then the loop over the other 13 | Schema + sample rows displayed at each step; per-table progress lines with counts during the loop |
| 5 | Run the final **verification cell** | The M1 frontend: a 14-row verdict table — `table · expected · actual · ✅/❌` — showing **14/14 ✅** and the 1,002,649 total |
| 6 | **Catalog Explorer → f1 → bronze → results** → *Sample Data* tab | Raw strings incl. literal `\N` values; `_source_file` and `_ingested_at` columns present |
| 7 | **Run the notebook a second time**, then re-run the verification cell | Identical counts — idempotency confirmed with your own eyes |
| 8 | Record the verdict-table results in this spec's Completion section; commit via the Git folder | Green checklist in §6; milestone done |

## 8. Constitution compliance

- **I (layering):** Bronze is raw + immutable; only ingest metadata added. ✅
- **II (contracts first):** this contract precedes any code. ✅
- **III (quality enforced):** completeness verified (counts); no filtering — Bronze's quality bar is
  "nothing lost, nothing invented". Row-level rules begin in M2. ✅
- **IV (Delta + UC):** all outputs `f1.bronze.*` Delta tables; the raw files live in a UC volume. ✅
- **V (one pipeline):** built as a hands-on notebook first; M4 re-expresses it in the single DLT
  pipeline (justified deviation: learning first, automation second — per M0 plan). ✅
- **VI (free tier):** one serverless session, ~28 MB data, no idle compute. ✅
- **Workflow (v1.2.0 hands-on & visible):** §7 runbook; verdict table + Catalog Explorer as the
  visible surface; operator runs everything. ✅

---

## ✅ Completion
- **Completed on:** 2026-07-15 — verified by the operator's own hands-on run in the workspace
  (constitution v1.2.0).
- **What was built:** catalog `f1` (5 schemas + `landing.ergast_csv` volume with the 14 uploaded
  CSVs) and 14 Bronze Delta tables `f1.bronze.<name>` via the teaching notebook
  [`src/bronze/01_bronze_ingest.py`](../src/bronze/01_bronze_ingest.py).
- **Acceptance criteria:** all met. Operator's verdict table: **14/14 tables ✅ · 1,002,649 rows
  accounted for** (the notebook's assert requires both `rows_ok` and `header_ok` on every table).
- **Actual output schema / row counts:** every table = all source columns as STRING (original
  camelCase names) + `_source_file` + `_ingested_at`; per-table counts equal the §3 contract exactly
  (78 … 876,204; total 1,002,649).
- **Quarantine / DQ results:** n/a for Bronze by design — completeness verified instead:
  `all_files_present` 14/14, `rowcount_matches_source` 14/14, `header_not_ingested` 14/14.
- **Deviations from spec & why:** one, minor — setup SQL runs as in-notebook `%sql` cells rather
  than the SQL Editor (simpler single-surface runbook; recorded in the changelog).
- **Commit(s):** `3e48e86` spec draft · `2828fe2` implementation · this commit (completion).

## Changelog
| Date | Change |
|------|--------|
| 2026-07-15 | Spec drafted |
| 2026-07-15 | Implementation built (`src/bronze/01_bronze_ingest.py`); §7 steps 1/3/4 updated — setup SQL is runnable in-notebook instead of the SQL Editor |
| 2026-07-15 | ✅ Completed — operator's run verified 14/14 tables, 1,002,649 rows |
