# M06 — Databricks SQL Dashboard Spec

| Field | Value |
|-------|-------|
| **Milestone** | M06 |
| **Status** | In Progress 🔨 (queries built — dashboard pending operator build) |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-16 |
| **Completed** | — |
| **Depends on** | M4 (production marts), M5 (governance) |

---

## 1. Objective

The payoff: an **AI/BI dashboard** on the production Gold marts (`f1.medallion.gold_*`) answering
five business questions — the artifact a stakeholder actually sees. No new data work happens here
*by design*: if the dashboard is easy, the lakehouse was built right.

## 2. Scope

**In scope:** versioned SQL datasets in [`sql/`](../sql/) (source of truth — the dashboard's
datasets are pasted from these files), the dashboard built **by the operator in the UI**
(widgets, layout, publish), sanity verification against pre-computed anchors.

**Out of scope:** embedding/sharing outside the workspace, alerts/schedules, any query on
non-gold layers (dashboards read the serving layer only — our own rule).

## 3. The business questions — question → dataset → widget → sanity anchor

| # | Question | Dataset (sql/) | Widget | Sanity anchor (pre-computed from source) |
|---|----------|----------------|--------|------------------------------------------|
| 0 | *How big is this dataset?* | `00_kpi_headline.sql` | 5 counters | **77** seasons · **1,171** GPs · **865** drivers · **1,161** wins · **22,475** stops |
| 1 | *Who dominated each era?* | `01_driver_eras.sql` | line (x=season, y=points, series=driver) | career leaders: Hamilton **5,082.5** · Verstappen **3,368.5** · Vettel **3,098** · Alonso **2,381** · Räikkönen **1,873** |
| 2 | *How have team fortunes shifted?* | `02_constructor_fortunes.sql` | line (series=constructor) | Ferrari **11,669.3** · Mercedes **8,440.6** · Red Bull **8,202** · McLaren **7,933.5** · Williams **3,776** |
| 3 | *How have pit stops evolved?* | `03_pit_stop_evolution.sql` | line (median + avg, seconds) | refuelling-ban drop after 2010; minimum median **≈22.3s (2012)** |
| 4 | *Who wins the most, ever?* | `04_wins_leaderboard.sql` | horizontal bar | Hamilton **106** · Schumacher **91** · Verstappen **71** |
| 5 | *What's each track's history?* | `05_circuit_history.sql` | table | Monza/Silverstone/Monaco at the top by races held |

Design rules carried from earlier milestones: `ORDER BY` lives **here**, not in the marts (M4's
note); points are **as recorded** (never re-scored across eras); all datasets read
`f1.medallion.gold_*` — the production path.

## 4. Acceptance criteria

- [ ] All 6 dataset queries versioned in `sql/` and pasted 1:1 into the dashboard.
- [ ] Dashboard answers **≥3 business questions** (we ship 5 + KPI row).
- [ ] Widgets match the §3 sanity anchors (spot-checked by the operator).
- [ ] Dashboard **published** (workspace default warehouse; serverless auto-stops after).
- [ ] Screenshot saved for M7 packaging.
- [ ] Operator personally built and verified every widget (constitution v1.2.0).
- [ ] Code committed; documented for a learner.

## 5. Hands-on run & verification (operator runbook)

| # | You do | You should see |
|---|--------|----------------|
| 1 | Pull `main` — the 6 files appear under `sql/` | Each file has a header saying which question it answers + its widget + anchors |
| 2 | **SQL Editor**: paste `00_kpi_headline.sql`, run on the serverless starter warehouse | One row: `77 · 1171 · 865 · 1161 · 22475` — smoke test passed |
| 3 | Sidebar → **Dashboards → Create dashboard** — name it **“F1 — 77 Seasons in Gold”** | Empty canvas with **Canvas** and **Data** tabs |
| 4 | **Data tab** → add data source → *Create from SQL* → paste each `sql/` file, name the dataset after the file (e.g. `driver_eras`) | 6 datasets listed |
| 5 | **Canvas tab** → add widgets per §3: counter row on top, the three lines, the bar, the table | Charts render with real data as you configure x/y/series |
| 6 | Spot-check anchors: Hamilton bar = 106; pit line dips to ~22.3 around 2012; Ferrari on top of the constructor lines | Numbers match — the marts (and everything under them) are telling the truth |
| 7 | **Publish** (top-right) | Published view — shareable inside the workspace |
| 8 | Take a full-page **screenshot** → save for M7 (`docs/`) | The money shot 📸 |
| 9 | Report back; Completion gets filled and committed | Milestone done |

**Free-tier notes:** the serverless starter SQL warehouse auto-stops — don't leave the editor
querying idly; build in one sitting.

## 6. Constitution compliance

- **I (layering):** dashboard reads Gold only — the layers exist precisely so this layer is trivial. ✅
- **II (contracts first):** questions + datasets + anchors specified before the build. ✅
- **III (quality):** anchors are pre-computed from source — widgets are verified, not admired. ✅
- **IV (Delta + UC):** all reads on governed `f1.medallion.*`. ✅
- **VI (free tier):** 1 serverless warehouse (the limit), auto-stop, one sitting. ✅
- **Workflow (v1.2.0):** §5 runbook; the dashboard IS the visible surface. ✅

---

## ✅ Completion  *(fill in when done)*
- **Completed on:** —
- **What was built:** —
- **Acceptance criteria:** —
- **Sanity anchors:** —
- **Deviations from spec & why:** —
- **Commit(s):** —

## Changelog
| Date | Change |
|------|--------|
| 2026-07-16 | Spec drafted; sql/ datasets built same day (anchors pre-computed from source) |
