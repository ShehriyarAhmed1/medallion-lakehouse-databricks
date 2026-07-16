# Project Overview — Medallion Lakehouse on Databricks

> **Status:** Planning
> **Owner:** Shehriyar Ahmed
> **Last updated:** 2026-07-15

---

## 1. Summary

Build an end-to-end **medallion (Bronze → Silver → Gold) lakehouse** on **Databricks Free Edition**
using the **Formula 1 (Ergast-schema)** dataset — 14 relational CSV files, ~1.0 million rows, covering
every F1 season from **1950 through the in-progress 2026 season**. Raw CSVs are uploaded into a Unity
Catalog **Volume**, landed as-is into an immutable **Bronze** layer, cleaned and correctly typed into a
trusted **Silver** layer, and aggregated into business-ready **Gold** marts. The whole batch flow is
orchestrated as a single **Lakeflow Declarative Pipeline (DLT)** with **data-quality expectations**,
governed by **Unity Catalog**, and served through a **Databricks SQL** dashboard.

## 2. Problem & Goal

Raw operational data is messy, inconsistently typed, and untrustworthy for analytics. The **goal** is to
turn a raw multi-table dataset into reliable, documented, query-ready business metrics using the
industry-standard medallion architecture — reproducibly, with quality enforced at every layer.

**Personal goal:** learn the Databricks lakehouse stack *deeply* (Delta Lake, PySpark, DLT, Unity
Catalog) by building a real, portfolio-grade project spec-first.

**Why F1 makes this a better learning project than a single-table source:**
- **14 related tables** with real foreign keys (`raceId`, `driverId`, `constructorId`, …) — Silver must
  conform a *relational model*, and Gold gets genuine join/star-schema work.
- **Real mess to clean:** the dump uses `\N` as its NULL marker, stores lap/race times as strings
  (`"1:27.452"`), and contains *scheduled future races* (rest of 2026) that legitimately have no results —
  perfect material for typed casts, quality expectations, and referential checks.
- **Meaningful business questions:** championships, race wins, pit-stop evolution — answers a reviewer
  can sanity-check against the real world.

## 3. What we're building — the medallion architecture

| Layer | Purpose | Rule |
|-------|---------|------|
| 🥉 **Bronze** | Raw, as-ingested copy of each CSV. Full history, nothing edited. | Immutable; ingest metadata only |
| 🥈 **Silver** | Cleaned, de-duplicated, schema-enforced, correctly typed "single source of truth". | Trusted, validated |
| 🥇 **Gold** | Aggregated, business-level marts ready for dashboards & analysts. | Consumption-ready |

Data flows **Bronze → Silver → Gold only** — never backward, never skipping a layer.

## 4. Dataset — Formula 1 (Ergast schema)

- **Source:** the classic Ergast F1 relational dataset (maintained today by community successors),
  exported as **14 CSV files** — snapshot dated **2026-07-05**, results through the 2026 British GP.
- **How it gets in:** Free Edition restricts outbound internet, so the CSVs are **uploaded from the
  local machine** into a Unity Catalog **Volume** via the workspace UI — no external network needed.
- **Coverage:** seasons **1950 → 2026** (2026 in progress: 198 of the season's result rows so far;
  remaining 2026 races exist in `races.csv` as schedule-only rows).

The 14 files, with verified row counts (excluding headers):

| File | Rows | What it is |
|------|------|------------|
| `races.csv` | 1,171 | One row per Grand Prix (year, round, circuit, date) — the central spine |
| `results.csv` | 27,436 | One row per driver per race — finishing position, points, laps |
| `drivers.csv` | 865 | Driver dimension (name, DOB, nationality) |
| `constructors.csv` | 214 | Constructor (team) dimension |
| `circuits.csv` | 78 | Circuit dimension (location, lat/lng, altitude) |
| `seasons.csv` | 77 | One row per season (1950–2026) |
| `status.csv` | 140 | Finishing-status lookup ("Finished", "Engine", "Collision", …) |
| `qualifying.csv` | 11,168 | Qualifying results (Q1/Q2/Q3 lap times as strings) |
| `sprint_results.csv` | 568 | Sprint-race results (2021+) |
| `lap_times.csv` | 876,204 | Every lap by every driver since 1996 — the volume table |
| `pit_stops.csv` | 22,475 | Every pit stop since 2011 (duration string + milliseconds) |
| `driver_standings.csv` | 35,559 | Championship standings after each race |
| `constructor_standings.csv` | 13,730 | Constructor standings after each race |
| `constructor_results.csv` | 12,964 | Constructor points per race |

**Total: 1,002,649 rows.** Known quirks to handle in Silver: `\N` NULL markers, duration/time strings,
future scheduled races without results, and standings tables that must reconcile with `results`.

## 5. Technology stack

Databricks Free Edition · Delta Lake · Lakeflow Declarative Pipelines (DLT) · Unity Catalog · PySpark ·
Databricks SQL · GitHub (version control) · GitHub Spec Kit (governing constitution only).

## 6. Architecture

```
F1 CSVs (local) ──upload──▶ UC Volume  /Volumes/f1/landing/ergast_csv/
                                 │  ingest (14 files, as-is)
                                 ▼
   ┌─────────┐   type · clean · conform · dedupe   ┌─────────┐   join · aggregate   ┌────────┐
   │ BRONZE  │ ──────────────────────────────────▶ │ SILVER  │ ───────────────────▶ │  GOLD  │
   │ 14 raw  │                                     │ trusted │                      │ marts  │
   └─────────┘                                     └────┬────┘                      └────────┘
        raw &                                           │  violating rows
        immutable                                       ▼
                                                 ┌──────────────┐
                                                 │  QUARANTINE  │  kept with a reason, never dropped
                                                 └──────────────┘
        └────────── one Lakeflow Declarative Pipeline (DLT) + expectations ──────────┘
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
| M1 | Bronze — CSV upload + raw ingestion (14 tables) | `specs/01-bronze.spec.md` | ✅ Done |
| M2 | Silver — type / clean / conform / dedupe | `specs/02-silver.spec.md` | ✅ Done |
| M3 | Gold — aggregated business marts | `specs/03-gold.spec.md` | ✅ Done |
| M4 | DLT pipeline + data-quality expectations | `specs/04-dlt-pipeline.spec.md` | ✅ Done |
| M5 | Unity Catalog governance | `specs/05-unity-catalog.spec.md` | ⬜ Not started |
| M6 | Databricks SQL dashboard | `specs/06-sql-dashboard.spec.md` | ⬜ Not started |
| M7 | Portfolio packaging | `specs/07-packaging.spec.md` | ⬜ Not started |

## 8. Success criteria

- A single DLT pipeline runs Bronze → Silver → Gold end-to-end and can be re-run reproducibly.
- Silver enforces schema, typing, and de-duplication; every layer declares data-quality expectations.
- Violating rows are quarantined with a logged count (nothing silently dropped); **row accounting
  closes** — Bronze counts = Silver + quarantine for every table.
- A Databricks SQL dashboard answers at least 3 business questions off the Gold tables
  (e.g. *who dominated each era?* · *how have constructor fortunes shifted?* · *how have pit stops evolved?*).
- Everything is version-controlled, structured, and documented for a reviewer to follow.

## 9. Constraints — Databricks Free Edition (free forever)

- Serverless-only; **1 workspace / 1 metastore**; **1 SQL warehouse (max 2X-Small)**; **5 concurrent
  job tasks**; **1 active DLT pipeline per type**.
- Daily/monthly compute quota — if exceeded, compute stops for the rest of the day (data & settings
  survive). We work in tight sessions and never leave compute idling.
- No external downloads → data is **uploaded from local CSVs** into a Unity Catalog Volume.
- Largest table (`lap_times`, 876K rows / ~25 MB) is comfortably within free-tier limits.

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
