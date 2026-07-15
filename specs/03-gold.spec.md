# M03 — Gold: Business Marts Spec

| Field | Value |
|-------|-------|
| **Milestone** | M03 |
| **Status** | In Progress 🔨 (notebook built — pending operator run) |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-15 |
| **Completed** | — |
| **Depends on** | M2 (Silver) |

---

## 1. Objective

Join and aggregate the trusted Silver model into **four small, dashboard-ready marts**, each answering
a concrete business question — with every mart **reconciling back to Silver counts**, so no number on
a future dashboard can drift from the trusted layer.

## 2. Scope

**In scope:** four `f1.gold.*` marts (contracts below), reconciliation verdict, and in-notebook
**charts** as the milestone's visible surface (first joins + first visualizations of the project).

**Out of scope:** the SQL dashboard itself (M6), DLT pipeline (M4), any new cleaning (Silver's job —
Gold *never* fixes data, it only aggregates trusted rows).

## 3. Data contract

**Input:** `f1.silver.results / races / drivers / constructors / circuits / pit_stops` (all verified in M2).

**Metric definitions (the business logic, decided here, once):**

| Metric | Definition | Why |
|--------|-----------|-----|
| win | `position = 1` | classified first |
| podium | `position <= 3` | NULL positions (DNFs) never count |
| pole | `grid = 1` | grid, not qualifying — penalties reshuffle the true start |
| dnf | `position IS NULL` | not classified (retired/disqualified/…) |
| best_finish | `MIN(position)` | NULL-safe: NULL if never classified |
| points | `SUM(points)` as **recorded** | 10 for a 2008 win, 25 today — never re-scored |

**Output marts:**

**`f1.gold.driver_season_summary`** — *who dominated each season/era?* Key: (season, driver_id)
| Column | Type | Note |
|--------|------|------|
| season | INT | from `races.year` |
| driver_id | INT | |
| driver | STRING | forename + surname |
| code / nationality | STRING | dimension attributes |
| races_entered | INT | count of result rows |
| wins / podiums / poles / dnfs | INT | per definitions above |
| points | DOUBLE | recorded points, rounded 1dp |
| best_finish | INT (nullable) | |

**`f1.gold.constructor_season_summary`** — *how have team fortunes shifted?* Key: (season, constructor_id)
Same shape at constructor level: `entries` (car-entries; 2 per race for a 2-car team), wins, podiums,
points, best_finish, nationality.

**`f1.gold.pit_stop_evolution`** — *how have pit stops changed?* Key: (season)
| season INT | stops BIGINT | avg_stop_s / median_stop_s / fastest_stop_s DOUBLE (from `milliseconds`/1000; the 3 NULL-duration stops count in `stops` but not in averages) |

**`f1.gold.circuit_stats`** — *what's the history of each track?* Key: (circuit_id)
| circuit_id INT | circuit / location / country STRING | races_held INT | first_season / last_season INT | distinct_winners INT (0 for scheduled-only) |

- **Write mode:** overwrite (idempotent snapshot), Delta, Unity Catalog.

## 4. Transformation / logic rules

1. Build one **enriched base**: `results ⋈ races(season) ⋈ drivers ⋈ constructors` — the project's
   first joins, possible *only because* Silver verified every FK (M2's orphan checks are what make
   inner joins here lossless).
2. Aggregate per mart with the §3 definitions. `COUNT(CASE WHEN …)` semantics: NULLs never count.
3. Gold never filters or repairs rows — if a number looks wrong here, the fix belongs in Silver rules.

## 5. Reconciliation — the acceptance gate (golden numbers pre-computed from source)

Every total below was computed locally from the CSVs (Silver = source minus quarantine, and
results/races/pit_stops had **zero** quarantine — so these are exact predictions):

| Check | Expected | Tolerance |
|-------|----------|-----------|
| driver mart rows | **3,254** · Σraces_entered = **27,436** (= silver.results) | exact |
| driver mart totals | Σwins **1,161** · Σpodiums **3,495** · Σpoles **1,168** · Σdnfs **10,953** | exact |
| Σpoints (driver mart) | **≈ 56,520.1** | **±0.5** (see below) |
| constructor mart rows | **1,132** · Σentries = **27,436** | exact |
| Σpoints (constructor mart) | ≈ driver mart's (triangle check: driver ≈ constructor ≈ silver) | **±0.5** |
| pit mart | **33 rows (1994–2026)** · Σstops = **22,475** (= silver.pit_stops) | exact |
| circuit mart | **78 rows** · Σraces_held = **1,171** (= silver.races) | exact |

> **Why points get a ±0.5 tolerance (discovered by the operator's first run — kept as a lesson):**
> in the 1950s drivers **shared cars**, so points were split into halves, thirds, even *sevenths*
> (the 1954 British GP fastest-lap point went to seven drivers, 1/7 ≈ 0.142857 each). Each mart
> rounds its groups to 1dp before summing, so the totals are rounding-path-dependent: raw
> **56,520.05** → driver path **56,519.8**, constructor path **56,520.0**. Exact float equality is
> the wrong test; counts stay exact. *(Same era quirk, other direction: total wins 1,161 exceeds
> raced races ~1,158 because a shared winning car = co-winners — informational, not asserted.)*
> Also: summed race points can legitimately differ from the official `driver_standings`
> (disqualifications, deductions) — that's why standings live as their own Silver table and Gold
> aggregates *results*, the raw truth.

## 6. Acceptance criteria

- [ ] Four marts exist at `f1.gold.*` with the contracted schemas.
- [ ] Reconciliation verdict all-✅ against the §5 golden numbers (asserted in-notebook).
- [ ] Charts render in-notebook: all-time wins top-10, points-by-season (Hamilton/Schumacher/
      Verstappen), pit-stop evolution — sanity: Hamilton 106 · Schumacher 91 · Verstappen 71 wins.
- [ ] Idempotent re-run (identical numbers).
- [ ] Operator personally executed every step and saw each result (constitution v1.2.0).
- [ ] Code committed; documented for a learner.

## 7. Hands-on run & verification (operator runbook)

| # | You do | You should see |
|---|--------|----------------|
| 1 | Pull `main` in the workspace Git folder; open `src/gold/03_gold_marts`; attach serverless | Notebook with teaching cells |
| 2 | Run the **enriched-base** cell | The Hamilton 2008 row, now with race name, season, driver & team names joined on — the first join of the project |
| 3 | Run the four **mart cells** one by one | Each mart written + a preview display (e.g. top driver-seasons by points) |
| 4 | Run the **reconciliation verdict** cell | The M3 frontend: every §5 golden number, expected vs actual, all ✅, asserts passing |
| 5 | Run the **chart cells** | Three charts: top-10 wins bar (Hamilton 106 on top), 3-driver points-per-season lines, pit-stop evolution line |
| 6 | **Catalog Explorer → f1 → gold** — browse the four marts' Sample Data | Small, readable, business-shaped tables |
| 7 | Re-run the whole notebook | Identical numbers (idempotent) |
| 8 | Report the verdict + what the charts show; Completion gets filled and committed | Milestone done |

## 8. Constitution compliance

- **I (layering):** reads Silver only, writes Gold only; no cleaning in Gold. ✅
- **II (contracts first):** metric definitions + mart schemas above precede the code. ✅
- **III (quality enforced):** reconciliation against pre-computed golden numbers, asserted. ✅
- **IV (Delta + UC):** all marts `f1.gold.*` Delta tables. ✅
- **V (one pipeline):** notebook first; M4 folds marts into the DLT graph. ✅
- **VI (free tier):** aggregations on ≤1M rows; single tight session. ✅
- **Workflow (v1.2.0):** §7 runbook; verdict + charts as the visible surface. ✅

---

## ✅ Completion  *(fill in when done)*
- **Completed on:** —
- **What was built:** —
- **Acceptance criteria:** —
- **Actual output schema / row counts:** —
- **Quarantine / DQ results:** — *(n/a — reconciliation verdict instead)*
- **Deviations from spec & why:** —
- **Commit(s):** —

## Changelog
| Date | Change |
|------|--------|
| 2026-07-15 | Spec drafted — golden numbers pre-computed from source; implementation built same day |
| 2026-07-15 | §5 amended after the operator's first run: points reconciliation now ±0.5 — exact float equality failed because 1950s shared-drive point splits (1/3, 1/7) make totals rounding-path-dependent; counts stay exact |
