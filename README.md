# Medallion Lakehouse on Databricks

An end-to-end **medallion architecture** (Bronze → Silver → Gold) lakehouse built on **Databricks Free Edition**, ingesting the public **NYC Taxi** dataset. Orchestrated with **Lakeflow Declarative Pipelines (DLT)**, governed by **Unity Catalog**, guarded by **data-quality expectations**, and served through a **Databricks SQL** dashboard.

This project is built **spec-driven**. Planning lives in [`planning/`](planning/) — the project [overview](planning/overview.md), the [master plan / deployment strategy](planning/masterplan.md), and the governing [constitution](planning/constitution.md). Each milestone then has its own spec in [`specs/`](specs/), written before it's built.

> 🚧 **Work in progress** — built in the open, spec first.

## Architecture

```
NYC Taxi (samples dataset)
        │  ingest
        ▼
   ┌─────────┐   clean · dedupe · schema    ┌─────────┐   aggregate    ┌────────┐
   │ BRONZE  │ ───────────────────────────▶ │ SILVER  │ ─────────────▶ │  GOLD  │
   │ raw     │                              │ trusted │                │ marts  │
   └─────────┘                              └─────────┘                └────────┘
        └──────── one Lakeflow Declarative Pipeline (DLT) + expectations ────────┘
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
| `docs/` | Diagrams & screenshots |

## Milestones

- [x] **M0** — Planning & repo setup
- [x] **M1** — Bronze (raw ingestion)
- [ ] **M2** — Silver (clean / dedupe / schema enforcement)
- [ ] **M3** — Gold (aggregated business marts)
- [ ] **M4** — DLT pipeline + data-quality expectations
- [ ] **M5** — Unity Catalog governance
- [ ] **M6** — Databricks SQL dashboard
- [ ] **M7** — Portfolio packaging
