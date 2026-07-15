# M01 — Bronze: CSV Upload + Raw Ingestion Spec

| Field | Value |
|-------|-------|
| **Milestone** | M01 |
| **Status** | Draft ⬜ |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-15 |
| **Completed** | — |
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

- [ ] Catalog `f1` with schemas `landing/bronze/silver/gold/quarantine` and volume `ergast_csv` exist.
- [ ] All 14 CSVs visible in the volume in Catalog Explorer.
- [ ] 14 tables exist at `f1.bronze.<table>` with all source columns as STRING + the 2 metadata columns.
- [ ] The verification cell shows **14/14 ✅** — every table's count equals the expected count above.
- [ ] Re-running the notebook changes no counts (idempotency proven by the operator's second run).
- [ ] Operator personally executed every step and saw each result (constitution v1.2.0).
- [ ] Code committed; documented for a learner.

## 7. Hands-on run & verification (operator runbook)

All steps are performed **by the operator, by hand**, in the Free Edition workspace. Tight session —
run it in one sitting, nothing left idling.

| # | You do | You should see |
|---|--------|----------------|
| 1 | Workspace → **SQL Editor** → run `CREATE CATALOG IF NOT EXISTS f1;` then the five `CREATE SCHEMA IF NOT EXISTS f1.<landing/bronze/silver/gold/quarantine>;` statements, then `CREATE VOLUME IF NOT EXISTS f1.landing.ergast_csv;` (statements provided in the notebook's first markdown cell) | Each statement returns OK; **Catalog Explorer** shows `f1` with 5 schemas and an empty volume under `landing` |
| 2 | **Catalog Explorer → f1 → landing → ergast_csv → Upload** — select all 14 CSVs from `~/Downloads/F1/` | 14 files listed in the volume with sizes (largest: `lap_times.csv` ≈ 25 MB) |
| 3 | **Git folder**: clone the GitHub repo (if not already) and open `src/bronze/01_bronze_ingest` | Notebook opens with teaching cells explaining each step |
| 4 | Attach serverless compute and **run the notebook cell by cell** (not Run-All) — read each markdown cell as you go | After each ingest step, a `display()` shows the table's first rows + its count |
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

## ✅ Completion  *(fill in when done)*
- **Completed on:** —
- **What was built:** —
- **Acceptance criteria:** —
- **Actual output schema / row counts:** —
- **Quarantine / DQ results:** — *(n/a for Bronze — completeness verdict table instead)*
- **Deviations from spec & why:** —
- **Commit(s):** —

## Changelog
| Date | Change |
|------|--------|
| 2026-07-15 | Spec drafted |
