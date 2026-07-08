# Project Overview — Medallion Lakehouse on Databricks

> **Status:** Planning
> **Owner:** Shehriyar Ahmed
> **Last updated:** 2026-07-08

---

## 1. Summary

Build an end-to-end **medallion (Bronze → Silver → Gold) lakehouse** on **Databricks Free Edition**
using the public **NYC Taxi** dataset. Raw trip data is ingested into an immutable **Bronze** layer,
cleaned and de-duplicated into a trusted **Silver** layer, and aggregated into business-ready **Gold**
marts. The whole batch flow is orchestrated as a single **Lakeflow Declarative Pipeline (DLT)** with
**data-quality expectations**, governed by **Unity Catalog**, and served through a **Databricks SQL**
dashboard.

## 2. Problem & Goal

Raw operational data is messy, duplicated, and untrustworthy for analytics. The **goal** is to turn a
raw public dataset into reliable, documented, query-ready business metrics using the industry-standard
medallion architecture — and to do it reproducibly, with quality enforced at every layer.

**Personal goal:** learn the Databricks lakehouse stack *deeply* (Delta Lake, PySpark, DLT, Unity
Catalog) by building a real, portfolio-grade project spec-first.

## 3. What we're building — the medallion architecture

| Layer | Purpose | Rule |
|-------|---------|------|
| 🥉 **Bronze** | Raw, as-ingested copy of the source. Full history, nothing edited. | Immutable; ingest metadata only |
| 🥈 **Silver** | Cleaned, de-duplicated, schema-enforced, correctly typed "single source of truth". | Trusted, validated |
| 🥇 **Gold** | Aggregated, business-level marts ready for dashboards & analysts. | Consumption-ready |

Data flows **Bronze → Silver → Gold only** — never backward, never skipping a layer.

## 4. Dataset — NYC Taxi

- **Source:** Databricks built-in sample dataset (`samples.nyctaxi.trips` and `/databricks-datasets/nyctaxi/`).
- **Why not download from the TLC website?** Free Edition restricts outbound internet to trusted
  domains, so we use the bundled sample data — zero network friction, always available.
- **Shape:** trip records with pickup/dropoff timestamps, distance, fare amount, and zone identifiers —
  rich enough for meaningful cleaning (Silver) and aggregation (Gold).

## 5. Technology stack

Databricks Free Edition · Delta Lake · Lakeflow Declarative Pipelines (DLT) · Unity Catalog · PySpark ·
Databricks SQL · GitHub (version control) · GitHub Spec Kit (governing constitution only).

## 6. Architecture

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

Bad rows caught by expectations are routed to a **quarantine** table, never silently dropped.

## 7. Milestones & spec map

Each milestone gets its own spec file in [`../specs/`](../specs/) and its own commit(s). See
[`specs/README.md`](../specs/README.md) for the spec lifecycle and commit conventions.

| # | Milestone | Spec file | Status |
|---|-----------|-----------|--------|
| M0 | Planning & repo setup | *(this folder)* | ✅ Done |
| M1 | Bronze — raw ingestion | `specs/01-bronze.spec.md` | ✅ Done |
| M2 | Silver — clean / dedupe / schema enforcement | `specs/02-silver.spec.md` | ✅ Done |
| M3 | Gold — aggregated business marts | `specs/03-gold.spec.md` | 📝 Spec drafted |
| M4 | DLT pipeline + data-quality expectations | `specs/04-dlt-pipeline.spec.md` | ⬜ Not started |
| M5 | Unity Catalog governance | `specs/05-unity-catalog.spec.md` | ⬜ Not started |
| M6 | Databricks SQL dashboard | `specs/06-sql-dashboard.spec.md` | ⬜ Not started |
| M7 | Portfolio packaging | `specs/07-packaging.spec.md` | ⬜ Not started |

## 8. Success criteria

- A single DLT pipeline runs Bronze → Silver → Gold end-to-end and can be re-run reproducibly.
- Silver enforces schema + de-duplication; every layer declares data-quality expectations.
- Violating rows are quarantined with a logged count (nothing silently dropped).
- A Databricks SQL dashboard answers at least 3 business questions off the Gold tables.
- Everything is version-controlled, structured, and documented for a reviewer to follow.

## 9. Constraints — Databricks Free Edition (free forever)

- Serverless-only; **1 workspace / 1 metastore**; **1 SQL warehouse (max 2X-Small)**; **5 concurrent
  job tasks**; **1 active DLT pipeline per type**.
- Daily/monthly compute quota — if exceeded, compute stops for the rest of the day (data & settings
  survive). We work in tight sessions and never leave compute idling.
- No external downloads → data comes from the built-in samples.

## 10. Repository structure

```
medallion-lakehouse-databricks/
├── README.md                 # public front door
├── planning/                 # M0 — planning phase
│   ├── overview.md           # ← this file: project info + overview
│   ├── masterplan.md         # deployment strategy (who / where / how)
│   └── constitution.md       # governing principles (non-negotiables)
├── specs/                    # one .spec file per milestone
│   ├── README.md             # spec lifecycle + commit conventions
│   ├── _TEMPLATE.spec.md     # template every milestone spec follows
│   └── NN-<milestone>.spec.md
├── src/                      # implementation (PySpark / DLT) — added per milestone
├── sql/                      # Databricks SQL dashboard queries
├── docs/                     # diagrams, screenshots
└── .specify/                 # Spec Kit engine (hidden; constitution tooling)
```

## 11. Working conventions

Spec-first, one milestone = one spec = one (or few) well-described commit(s); completed milestones get a
**Completion** section written back into their spec. Full rules in
[`specs/README.md`](../specs/README.md) and [`planning/constitution.md`](constitution.md).
