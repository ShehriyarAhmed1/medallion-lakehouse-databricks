# Medallion Lakehouse (Formula 1) — Plan & Progress

> **A single-page briefing for review:** *what* this project is, *how* it's being built, and exactly
> where it stands today. Source row counts below are verified from the actual CSV files; lakehouse
> figures will be filled in as each milestone is verified in the workspace. Full detail lives in
> [`planning/`](planning/) and [`specs/`](specs/).

## 1. At a glance

| Metric | Value | Meaning |
|--------|-------|---------|
| Milestones complete | **1 / 8** (M0 done) | Planning re-done for the F1 dataset; build not started |
| Dataset | **Formula 1 (Ergast schema)** | 14 relational CSVs, snapshot 2026-07-05 |
| Coverage | **1950 → 2026 (in progress)** | results through the 2026 British GP |
| Source rows | **1,002,649** | verified by counting the actual files |
| Largest table | `lap_times` — **876,204 rows** | every lap by every driver since 1996 |

**Previous build:** the same architecture was fully built and verified on the NYC Taxi sample
(M1–M3 verified, M4 pending) — preserved on branch `archive/nyc-taxi`. This round rebuilds it
teaching-first on a richer, multi-table dataset.

---

## 2. What we're building — the data flow

An end-to-end **medallion architecture**: raw CSVs land in **Bronze**, are cleaned and typed into a
trusted **Silver** layer, then joined and aggregated into business-ready **Gold** marts. Data flows
**forward only** — never backward, never skipping a layer. Bad rows are routed to a **quarantine**
table with a reason, never silently dropped.

```
F1 CSVs (local) ──upload──▶ UC Volume  /Volumes/f1/landing/ergast_csv/
                                 │  ingest (14 files, as-is)
                                 ▼
   ┌─────────┐   type · clean · conform · dedupe   ┌─────────┐   join · aggregate   ┌────────┐
   │ BRONZE  │ ──────────────────────────────────▶ │ SILVER  │ ───────────────────▶ │  GOLD  │
   │ 14 raw  │                                     │ trusted │                      │ marts  │
   └─────────┘                                     └────┬────┘                      └────────┘
      raw &                                             │  violating rows
      immutable                                         ▼
                                                 ┌──────────────┐
                                                 │  QUARANTINE  │  kept with a reason
                                                 └──────────────┘
        └────────── one Lakeflow Declarative Pipeline (DLT) + expectations ──────────┘
                                                                              │
                                                                Databricks SQL dashboard
```

Every layer is a governed **Delta** table under Unity Catalog, addressed as `f1.<layer>.<table>`
(e.g. `f1.silver.results`).

| Layer | Purpose | Rule |
|-------|---------|------|
| 🥉 **Bronze** | Raw, as-ingested copy of each CSV. Nothing edited. | Immutable; ingest metadata only |
| 🥈 **Silver** | Cleaned, typed, de-duplicated "single source of truth". | Trusted, validated |
| 🥇 **Gold** | Joined & aggregated business marts for dashboards. | Consumption-ready |

**Stack:** Databricks Free Edition · Delta Lake · Lakeflow Declarative Pipelines (DLT) · Unity Catalog ·
PySpark · Databricks SQL · GitHub (version control).

---

## 3. The dataset — why F1 is a step up

14 related tables with real foreign keys (`raceId`, `driverId`, `constructorId`, …), and real mess to
clean — exactly what Silver and the quality expectations are for:

| Quirk in the raw data | What the lakehouse does about it |
|-----------------------|----------------------------------|
| `\N` used as the NULL marker | Silver converts to real NULLs during typed casts |
| Times stored as strings (`"1:27.452"`) | Silver keeps the provided `milliseconds` column as the numeric truth |
| Future 2026 races exist with no results | Expectations distinguish *scheduled* from *raced*; nothing falsely quarantined |
| Standings must agree with results | Cross-table reconciliation checks in Silver/Gold |

Key tables (verified row counts): `races` 1,171 · `results` 27,436 · `drivers` 865 · `constructors` 214 ·
`circuits` 78 · `qualifying` 11,168 · `pit_stops` 22,475 · `lap_times` 876,204 · plus standings and
lookup tables — **1,002,649 rows total**.

---

## 4. The roadmap — milestone by milestone

Working method: **one milestone = one spec, written before the code**, then verified against its
acceptance criteria before it's marked done — the same *verify-then-mark-done* gate proven in round 1.

| # | Milestone | Status | Spec |
|---|-----------|--------|------|
| M0 | Planning & repo setup (F1 re-plan) | ✅ Done | [`planning/`](planning/) |
| M1 | Bronze — CSV upload + raw ingestion (14 tables) | 🔨 Notebook built — pending operator run | [`specs/01-bronze.spec.md`](specs/01-bronze.spec.md) |
| M2 | Silver — type / clean / conform / dedupe | ⬜ Planned | `specs/02-silver.spec.md` |
| M3 | Gold — business marts | ⬜ Planned | `specs/03-gold.spec.md` |
| M4 | DLT pipeline + expectations | ⬜ Planned | `specs/04-dlt-pipeline.spec.md` |
| M5 | Unity Catalog governance | ⬜ Planned | — |
| M6 | Databricks SQL dashboard | ⬜ Planned | — |
| M7 | Portfolio packaging | ⬜ Planned | — |

### ✅ M0 — Planning (re-done for F1)
- Dataset locked: the Ergast-schema F1 CSV snapshot (verified file-by-file, counts above).
- Planning docs rewritten: [`overview`](planning/overview.md) · [`masterplan`](planning/masterplan.md) ·
  [`constitution v1.1.0`](planning/constitution.md) (data-source amendment; principles unchanged).
- Namespace locked: catalog `f1`, schemas `landing / bronze / silver / gold / quarantine`.

### 🔨 M1 — Bronze *(in progress — the operator's first hands-on session)*
Upload the 14 CSVs to `/Volumes/f1/landing/ergast_csv/`; ingest each as-is into `f1.bronze.<table>`
with only `_source_file` + `_ingested_at` added. **Built:** teaching notebook
[`src/bronze/01_bronze_ingest.py`](src/bronze/01_bronze_ingest.py) (setup cells → upload check →
`circuits` walkthrough → loop over 13 → 14/14 verdict table). **Pending:** the operator runs the
spec's §7 runbook by hand. **Acceptance:** every Bronze table's row count equals its source CSV
exactly; idempotent re-run.

### ⬜ M2 — Silver
Typed casts (`\N` → NULL, dates, numerics), de-duplication on declared natural keys, referential and
range expectations; violating rows quarantined with a reason. **Row accounting must close** per table.

### ⬜ M3 — Gold
Joined, dashboard-ready marts answering concrete questions — planned candidates: driver season/career
summaries, constructor season standings, pit-stop evolution by season, circuit statistics. Final list
locked in the M3 spec; every mart must reconcile back to Silver counts.

### ⬜ M4–M7
One DLT pipeline with native expectations (M4) → governance comments/tags/grants (M5) → SQL dashboard
answering ≥3 business questions (M6) → portfolio packaging with diagrams & screenshots (M7).

---

## 5. The rules the project runs by (constitution)

Six principles every spec and implementation must comply with — unchanged from round 1
(v1.1.0 amends only the data source). Full text: [`planning/constitution.md`](planning/constitution.md).

| # | Principle | In short |
|---|-----------|----------|
| I | **Medallion layering is sacred** *(non-negotiable)* | Bronze raw/immutable, Silver clean/typed, Gold aggregated; forward-only flow. |
| II | **Contracts before code** | Each table's schema is written in the spec *before* the table is built. |
| III | **Data quality is enforced** *(non-negotiable)* | Violating rows are quarantined/dropped with a logged count — never silently passed on. |
| IV | **Delta + Unity Catalog, always** | All tables are Delta, addressed by full `catalog.schema.table`. No loose files. |
| V | **One reproducible pipeline** | The whole batch flow is one Lakeflow pipeline; a full refresh reproduces identical Gold. |
| VI | **Free-tier discipline** | Fits Databricks Free Edition; no idle compute; every artifact pushed to Git. |

---

## 6. How it ships — deployment plan

GitHub is the source of truth; Databricks mirrors the repo via **Git folders**. Data takes the only
road Free Edition allows: a one-time **UI upload** of the CSVs into a Unity Catalog volume.
Full strategy: [`planning/masterplan.md`](planning/masterplan.md).

```
GitHub repo ──git──▶ Databricks Git folder ──▶ DLT pipeline + notebooks + SQL dashboard
Local F1 CSVs ──UI upload──▶ /Volumes/f1/landing/ergast_csv/
```

**Free-tier guardrails baked into the design:** serverless only · 1 workspace / 1 metastore ·
1 SQL warehouse (max 2X-Small) · 1 active DLT pipeline per type · daily compute quota (tight sessions,
no idle compute) · no external downloads (local CSV upload) · reproducible via full-refresh +
Delta time-travel rollback.

---

## 7. Immediate next step

**The operator's first hands-on workspace session** — run the M1 runbook
([spec §7](specs/01-bronze.spec.md)): pull the repo into a Databricks Git folder, run the notebook's
setup cells, upload the 14 CSVs, ingest cell by cell, and get the **14/14 ✅ verdict table**; run it
a second time to prove idempotency, then record results in the spec's Completion section.

---

## 8. Where to find the full detail

- **Architecture & flow diagrams (north star):** [`docs/architecture.md`](docs/architecture.md)
- **Planning:** [`planning/overview.md`](planning/overview.md) · [`planning/masterplan.md`](planning/masterplan.md) · [`planning/constitution.md`](planning/constitution.md)
- **Specs (one per milestone):** [`specs/`](specs/)
- **Implementation:** `src/` *(added from M1 onward)*
- **Round-1 build (NYC Taxi, fully verified):** branch `archive/nyc-taxi`
