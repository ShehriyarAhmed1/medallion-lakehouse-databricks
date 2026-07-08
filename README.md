# Medallion Lakehouse on Databricks

An end-to-end **medallion architecture** (Bronze → Silver → Gold) lakehouse built on **Databricks Free Edition**, ingesting the public **NYC Taxi** dataset. Orchestrated with **Lakeflow Declarative Pipelines (DLT)**, governed by **Unity Catalog**, guarded by **data-quality expectations**, and served through a **Databricks SQL** dashboard.

This project is built **spec-driven** using [GitHub Spec Kit](https://github.com/github/spec-kit): the governing principles live in [`.specify/memory/constitution.md`](.specify/memory/constitution.md) and the specification, plan, and task breakdown live in [`specs/`](specs/).

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

## Status

- [ ] Spec — constitution, spec, plan, tasks
- [ ] Bronze — raw ingestion
- [ ] Silver — cleaning, dedupe, schema enforcement
- [ ] Gold — aggregated business marts
- [ ] DLT pipeline + data-quality expectations
- [ ] Unity Catalog governance
- [ ] Databricks SQL dashboard
