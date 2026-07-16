# Medallion Lakehouse on Databricks — Formula 1

An end-to-end **medallion architecture** (Bronze → Silver → Gold) lakehouse built on **Databricks Free Edition**, ingesting the classic **Formula 1 (Ergast-schema)** dataset — 14 relational CSVs, ~1.0M rows, every season from 1950 to today. Orchestrated with **Lakeflow Declarative Pipelines (DLT)**, governed by **Unity Catalog**, guarded by **data-quality expectations**, and served through a **Databricks SQL** dashboard.

This project is built **spec-driven**. Planning lives in [`planning/`](planning/) — the project [overview](planning/overview.md), the [master plan / deployment strategy](planning/masterplan.md), and the governing [constitution](planning/constitution.md). Each milestone then has its own spec in [`specs/`](specs/), written before it's built.

> 🚧 **Work in progress** — built in the open, spec first.

## Architecture

```
F1 CSVs (local) ──upload──▶ UC Volume  /Volumes/f1/landing/ergast_csv/
                                 │  ingest (14 files, as-is)
                                 ▼
   ┌─────────┐   type · clean · conform · dedupe   ┌─────────┐   join · aggregate   ┌────────┐
   │ BRONZE  │ ──────────────────────────────────▶ │ SILVER  │ ───────────────────▶ │  GOLD  │
   │ 14 raw  │                                     │ trusted │                      │ marts  │
   └─────────┘                                     └────┬────┘                      └────────┘
                                                        │  violating rows → QUARANTINE (with reason)
        └────────── one Lakeflow Declarative Pipeline (DLT) + expectations ──────────┘
                                                                              │
                                                                Databricks SQL dashboard
```

## Stack

Databricks Free Edition · Delta Lake · Lakeflow Declarative Pipelines (DLT) · Unity Catalog · PySpark · Databricks SQL

## Repository map

| Path | What |
|------|------|
| [`planning/`](planning/) | Project overview, deployment master plan, constitution |
| [`specs/`](specs/) | One spec per milestone (spec-first) |
| `src/` | PySpark / DLT implementation (added per milestone) |
| `sql/` | Databricks SQL dashboard queries |
| [`docs/architecture.md`](docs/architecture.md) | **The north-star flow diagram** + layer-by-layer walkthrough (read this first) |

## Milestones

- [x] **M0** — Planning & repo setup
- [x] **M1** — Bronze (CSV upload + raw ingestion, 14 tables — 14/14 verified, 1,002,649 rows)
- [x] **M2** — Silver (typed & deduped: 1,000,396 trusted + 2,253 quarantined with reasons — accounting closes)
- [x] **M3** — Gold (4 marts, 14-check reconciliation to Silver, first charts — Hamilton 106 · Schumacher 91 · Verstappen 71)
- [x] **M4** — DLT pipeline + data-quality expectations (one 61-node pipeline; audits all-true; full refresh reproduces identical numbers)
- [x] **M5** — Unity Catalog governance (100% column docs on silver+gold; tags; reviewer grants)
- [ ] **M6** — Databricks SQL dashboard
- [ ] **M7** — Portfolio packaging

> **Note:** a previous, fully verified build of this same architecture on the NYC Taxi sample dataset
> is preserved on the [`archive/nyc-taxi`](../../tree/archive/nyc-taxi) branch.
