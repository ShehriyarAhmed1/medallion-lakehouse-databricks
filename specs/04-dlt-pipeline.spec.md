# M04 — Lakeflow Declarative Pipeline + Expectations Spec

| Field | Value |
|-------|-------|
| **Milestone** | M04 |
| **Status** | ✅ Completed |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-16 |
| **Completed** | 2026-07-16 |
| **Depends on** | M1–M3 (verified layer logic + numbers) |

---

## 1. Objective

Re-express the verified Bronze → Silver → Gold flow as **one Lakeflow Declarative Pipeline (DLT)**:
press Start, and the entire medallion rebuilds itself in the right order, with the M2 quality rules
running as **native expectations** (pass-rates visible in the pipeline UI) and an in-pipeline **audit
dataset** proving the run reproduced the already-verified numbers. This completes Constitution V —
*one reproducible pipeline*.

## 2. Scope

**In scope:** the pipeline source (`src/pipelines/f1_medallion_pipeline.py`, self-contained),
expectations, two audit datasets, operator runbook.

**Out of scope:** dashboard (M6), governance metadata (M5). The M1–M3 notebooks are **not deleted**
— they remain the verified, hands-on teaching prototypes; from M4 onward the **pipeline is the
production path**.

## 3. Design decisions (the important ones)

1. **Target = single schema `f1.medallion`, prefixed dataset names** (`bronze_races`,
   `silver_races`, `quarantine_races`, `gold_driver_season_summary`, …) — the pattern verified
   against the Lakeflow docs in round 1. Why: datasets reference each other by short name
   (`spark.read.table("silver_races")`), the pipeline needs one target schema in its settings, and —
   crucially — it **never conflicts with the notebook-built tables** in `f1.bronze/silver/gold/quarantine`.
   Nothing is dropped; the runbook is non-destructive.
2. **Expectations strategy — one `quality_gate` per Silver table.** The M2 rule engine (casts,
   required, domain rules, FK checks, dedup) computes `_reasons` in an intermediate view; the Silver
   dataset then declares `@dp.expect_all_or_drop("quality_gate", "_reasons = ''")`. DLT does the
   dropping *and* reports the per-table pass-rate in the UI. Fine-grained reasons stay queryable in
   the quarantine datasets. (A per-rule expectation set can't express window-dedup or FK anti-joins —
   the gate pattern is the standard Lakeflow quarantine idiom.)
3. **Schema amendment (silver):** the gate column rides along, so every `silver_*` dataset carries
   `_reasons` — **constant `''` by construction** (the expectation guarantees it). Documented here;
   downstream marts select named columns and are unaffected.
4. **Quarantine = the exact complement**, from the same intermediate view: original string columns +
   `_reasons` + `_quarantined_at`. Expectations drop-and-count; quarantine *keeps* (Constitution III).
5. **The verdict lives IN the pipeline**: two audit materialized views —
   `audit_row_accounting` (per table: bronze = silver + quarantine, and quarantine == M2's verified
   counts) and `audit_gold_reconciliation` (mart rows + key sums vs M3's golden numbers). The
   pipeline graph itself ends in its own proof.

## 4. Data contract — datasets (all in `f1.medallion`)

| Group | Datasets | Contract |
|-------|----------|----------|
| 🥉 `bronze_<table>` ×14 | MV per CSV | identical to M1: all STRING + `_source_file`, `_ingested_at` |
| `staged_<table>` ×14 | **temporary views** (internal) | M2 engine: originals + typed cols + `_reasons` |
| 🥈 `silver_<table>` ×14 | MV + `quality_gate` expectation | identical to M2 contract **+ `_reasons` = ''** (§3.3) |
| 🚧 `quarantine_<table>` ×14 | MV | original strings + `_reasons` + `_quarantined_at` |
| 🥇 `gold_*` ×4 | MV | identical to M3 mart contracts (no ORDER BY — ordering belongs to the dashboard) |
| 🔎 `audit_row_accounting`, `audit_gold_reconciliation` | MV | the M4 verdict tables (§5) |

## 5. Expectations & predicted metrics (from the verified M1–M3 runs)

| Where | Expectation | Predicted result |
|-------|-------------|------------------|
| `silver_lap_times` | `quality_gate` | **99.74% kept** — 873,953 kept / **2,251 dropped** |
| `silver_sprint_results` | `quality_gate` | 566 kept / **2 dropped** |
| all other 12 `silver_*` | `quality_gate` | **100%** — 0 dropped |
| `audit_row_accounting` | 14 rows | every row `closes = true` and `as_predicted = true` |
| `audit_gold_reconciliation` | 4 rows | rows 3,254 / 1,132 / 33 / 78 and Σentries 27,436 ·  Σstops 22,475 · Σraces_held 1,171 — all `ok = true` |

## 6. Acceptance criteria

- [x] Pipeline runs **green end-to-end on serverless** (the one allowed pipeline — Free Edition).
- [x] Expectation gate results match §5 — verified via `audit_row_accounting` + quarantine complement
      (2,251 dropped on lap_times, 2 on sprint_results, 0 elsewhere); see Completion for the UI-tab note.
- [x] `audit_row_accounting`: 14/14 `closes` & `as_predicted` true (operator-pasted output on record).
- [x] `audit_gold_reconciliation`: 4/4 `ok` true (operator-pasted output on record).
- [x] **Full refresh reproduces identical numbers** (Constitution V's literal test — operator-confirmed).
- [x] Operator personally created, ran, and inspected the pipeline (constitution v1.2.0).
- [x] Code committed; documented for a learner.

## 7. Hands-on run & verification (operator runbook)

| # | You do | You should see |
|---|--------|----------------|
| 1 | Pull `main` in the workspace Git folder | `src/pipelines/f1_medallion_pipeline.py` appears |
| 2 | SQL Editor: `CREATE SCHEMA IF NOT EXISTS f1.medallion;` | OK |
| 3 | **Jobs & Pipelines → Create → ETL pipeline**: name `f1-medallion-pipeline` · serverless · **Source code** = the Git-folder path to `src/pipelines/f1_medallion_pipeline.py` · **Default catalog** `f1` · **Default schema** `medallion` | Pipeline created (not yet run) |
| 4 | Press **Start** | The graph builds itself: bronze → staged → silver (+quarantine) → gold → audit, nodes turning green; `lap_times` branch is the slowest |
| 5 | Click `silver_lap_times` → **Data quality** | `quality_gate` ≈ **99.74%**, 2,251 records dropped — the M2 rules, now native metrics |
| 6 | Click `silver_sprint_results` → Data quality | 2 records dropped |
| 7 | SQL Editor: `SELECT * FROM f1.medallion.audit_row_accounting ORDER BY table;` | 14 rows, `closes = true` and `as_predicted = true` everywhere |
| 8 | `SELECT * FROM f1.medallion.audit_gold_reconciliation;` | 4 rows, all `ok = true` |
| 9 | **Start ▾ → Full refresh** (the reproducibility test) | Same green graph, same expectation numbers, same audit values |
| 10 | Report the numbers here | Completion gets filled and committed |

**Free-tier notes:** one pipeline (the limit); serverless; stop nothing manually — DLT tears down
compute when the update finishes. If the run fails on the import line, the workspace channel may
predate the `pyspark.pipelines` module — fallback mapping is documented at the top of the pipeline
file (classic `dlt` names), and the deviation gets recorded here.

## 8. Constitution compliance

- **I (layering):** the graph *is* the layering — edges only point forward. ✅
- **II (contracts first):** dataset contracts above; layer logic identical to verified M1–M3. ✅
- **III (quality enforced):** native expectations with visible pass-rates + quarantine complement. ✅
- **IV (Delta + UC):** all datasets Delta under `f1.medallion`. ✅
- **V (one pipeline):** this milestone is Rule V. Full refresh = the reproducibility proof. ✅
- **VI (free tier):** 1 pipeline, serverless, auto-teardown. ✅
- **Workflow (v1.2.0):** runbook §7; the pipeline graph + Data-quality tabs + audit tables are the
  visible surface. ✅

---

## ✅ Completion
- **Completed on:** 2026-07-16 — operator created and ran the pipeline hands-on (constitution v1.2.0).
- **What was built:** `f1-medallion-pipeline` (serverless), source =
  [`src/pipelines/f1_medallion_pipeline.py`](../src/pipelines/f1_medallion_pipeline.py) attached from
  the Git folder; 61-node graph: 14 bronze + 14 staged views + 14 gated silver + 14 quarantine +
  4 gold marts + 2 audits, all in `f1.medallion`.
- **Acceptance criteria:** all met. `audit_row_accounting` **14/14** `closes` + `as_predicted`
  (lap_times 876,204 = 873,953 + 2,251 · sprint_results 568 = 566 + 2 · rest N + 0);
  `audit_gold_reconciliation` **4/4 ok** (3,254 / 1,132 / 33 / 78 rows; key sums 27,436 · 27,436 ·
  22,475 · 1,171). **Full refresh reproduced identical numbers — Rule V proven.**
- **Actual output schema / row counts:** identical to the verified M1–M3 contracts (+ the documented
  `_reasons` gate column on silver datasets).
- **Quarantine / DQ results:** the `quality_gate` expectations dropped exactly the verified counts;
  quarantine datasets hold the complement with reasons.
- **Deviations from spec & why:** two, both recorded honestly. (1) The workspace opened the **new
  Lakeflow Pipelines Editor**, so runbook steps 3–4 differed: catalog/schema set via the top-bar
  `f1.medallion` chip, source attached via **⋮ → "Add existing source code"** (Git-folder file),
  scaffold `my_transformation.py` deleted; serverless is implicit on Free Edition (no selector).
  (2) The per-node **Data quality tab** metric wasn't visually located in the new UI; the gate's
  numbers were verified through the audit table + quarantine counts instead — an equivalent
  (arguably stronger) check.
- **Commit(s):** `a873c12` spec · `a6a4a12` implementation · this commit (completion).

## Changelog
| Date | Change |
|------|--------|
| 2026-07-16 | Spec drafted (single-schema `f1.medallion` design, gate-pattern expectations, in-pipeline audit); implementation built same day |
| 2026-07-16 | ✅ Completed — operator's run + full refresh: both audits all-true, identical numbers twice |
