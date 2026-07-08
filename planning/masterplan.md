# Master Plan — Deployment Strategy

> **Status:** Planning
> **Owner:** Shehriyar Ahmed
> **Last updated:** 2026-07-08
> **Scope:** *Who* deploys, *where* it runs, and *how* it gets there.

This document answers the operational questions: where does this project live, how does code get from
this GitHub repo into a running lakehouse, and how do we keep it reproducible and inside the free tier.

---

## 1. Who

| Role | Person | Responsibility |
|------|--------|----------------|
| Developer / Data Engineer | Shehriyar Ahmed | Writes specs, notebooks, DLT pipeline, dashboard |
| Operator / Deployer | Shehriyar Ahmed | Runs the pipeline, manages the workspace, monitors quota |
| Reviewer | Manager | Reviews specs, structure, and completed milestones |

Single-owner project — the developer is also the operator. (Section 8 notes how roles would split in a
real production team.)

## 2. Where — target environment

| Concern | Choice |
|---------|--------|
| Platform | **Databricks Free Edition** (serverless, free forever) |
| Region / cloud | Managed by Databricks (Free Edition; no cloud account needed) |
| Governance root | **Unity Catalog** — 1 metastore (auto-provisioned) |
| Catalog | `nyc_taxi` (created in-workspace) |
| Schemas | `bronze`, `silver`, `gold`, `quarantine` |
| Compute | Serverless (notebooks + DLT); **1 SQL warehouse, 2X-Small** for the dashboard |
| Source data | Built-in `samples.nyctaxi.trips` (no external network) |

**Namespace convention:** every table is addressed as `nyc_taxi.<layer>.<table>`
(e.g. `nyc_taxi.silver.trips_clean`).

## 3. What gets deployed

1. **Layer logic** — PySpark / DLT transformation code in `src/` (Bronze, Silver, Gold).
2. **One Lakeflow Declarative Pipeline (DLT)** — wires Bronze → Silver → Gold with expectations and a
   quarantine table. (Free Edition allows 1 active pipeline per type.)
3. **SQL dashboard** — queries in `sql/`, published as a Databricks SQL dashboard on the Gold tables.
4. **Governance metadata** — catalog/schema/table comments and tags applied via Unity Catalog.

## 4. How — deployment runbook

Code lives in **GitHub** (source of truth) and is brought into Databricks via **Git folders**
(Databricks Repos), so the workspace mirrors the repo — no copy-paste drift.

```
GitHub (this repo)  ──git──▶  Databricks Git folder  ──▶  DLT pipeline + notebooks + SQL
      ▲                                                          │
      └──────────────── commit specs & code ◀────────────────────┘
```

**Steps:**
1. Create the Databricks Free Edition workspace (one-time).
2. In the workspace: **Git folders → clone** `https://github.com/ShehriyarAhmed1/medallion-lakehouse-databricks`.
3. Create the Unity Catalog objects: catalog `nyc_taxi` + schemas `bronze/silver/gold/quarantine`.
4. Create a **DLT pipeline** pointing at the pipeline source in `src/`; set target catalog/schema.
5. **Run** the pipeline (triggered/manual). Verify expectations & quarantine counts.
6. Attach the pre-created **serverless SQL warehouse** and build the dashboard from `sql/`.
7. Commit any changes back to GitHub (specs updated, screenshots into `docs/`).

> **Note on Databricks Asset Bundles (DAB):** the "proper" CI/CD way to deploy pipelines/jobs as code.
> We keep the repo DAB-ready (declarative pipeline definition in `src/`) but, given a single Free Edition
> workspace and a tight timeline, deploy via Git folders + the UI for v1. DAB/CI is listed as future work.

## 5. Environments

Free Edition gives **one** workspace, so we run a **single environment**. We simulate separation with
**schemas** and Delta features rather than multiple workspaces:

| Real-world | This project (Free Edition) |
|------------|-----------------------------|
| dev / staging / prod workspaces | one workspace |
| prod catalog vs dev catalog | one `nyc_taxi` catalog, layered schemas |
| blue/green deploys | DLT **full refresh** re-materializes tables |

## 6. Orchestration & scheduling

- v1: DLT pipeline run **manually / triggered** (keeps compute usage deliberate).
- Optional: a scheduled trigger (respecting the 5-concurrent-task limit) once the pipeline is stable.

## 7. Cost & quota management (free-tier discipline)

- Use the smallest compute (serverless defaults; 2X-Small SQL warehouse).
- **Never leave compute idling** — stop the warehouse when not querying.
- Watch the daily quota; if compute cuts off, data & settings survive — resume next day.
- **Everything is pushed to GitHub** so nothing is trapped if the workspace compute is unavailable.

## 8. Reproducibility, rollback & recovery

- **Reproducible:** a clean DLT full-refresh rebuilds identical Gold from the same source.
- **Rollback:** Delta **time travel** (`VERSION AS OF`) to inspect/restore prior table states.
- **Recovery:** code + specs in GitHub; workspace objects can be re-created from the runbook above.

## 9. Security & governance

- Unity Catalog owns access control (single-user here; documented grants for the reviewer).
- No secrets in the repo (`.gitignore` covers keys/tokens); the internal project-list PDF is not published.

## 10. Future / production hardening (out of scope for v1)

- Databricks Asset Bundles + GitHub Actions for CI/CD deploys.
- Separate dev/prod catalogs; automated pipeline scheduling & alerting.
- Incremental/streaming ingestion (Auto Loader / streaming tables) — see project #11.
