# Master Plan — Deployment Strategy

> **Status:** Planning
> **Owner:** Shehriyar Ahmed
> **Last updated:** 2026-07-15
> **Scope:** *Who* deploys, *where* it runs, and *how* it gets there.

This document answers the operational questions: where does this project live, how do code **and data**
get from the developer's machine into a running lakehouse, and how do we keep it reproducible and inside
the free tier.

---

## 1. Who

| Role | Person | Responsibility |
|------|--------|----------------|
| Developer / Data Engineer | Shehriyar Ahmed | Writes specs, notebooks, DLT pipeline, dashboard |
| Operator / Deployer | Shehriyar Ahmed | Uploads CSVs, runs the pipeline, manages the workspace, monitors quota |
| Reviewer | Manager | Reviews specs, structure, and completed milestones |

Single-owner project — the developer is also the operator. (Section 8 notes how roles would split in a
real production team.)

## 2. Where — target environment

| Concern | Choice |
|---------|--------|
| Platform | **Databricks Free Edition** (serverless, free forever) |
| Region / cloud | Managed by Databricks (Free Edition; no cloud account needed) |
| Governance root | **Unity Catalog** — 1 metastore (auto-provisioned) |
| Catalog | `f1` (created in-workspace) |
| Schemas | `landing` (raw file volume), `bronze`, `silver`, `gold`, `quarantine` |
| Landing volume | `/Volumes/f1/landing/ergast_csv/` — the 14 uploaded CSVs |
| Compute | Serverless (notebooks + DLT); **1 SQL warehouse, 2X-Small** for the dashboard |
| Source data | **Local F1 CSVs uploaded via the workspace UI** (no external network) |

**Namespace convention:** every table is addressed as `f1.<layer>.<table>`
(e.g. `f1.silver.results`). The layer lives in the schema name, so table names stay clean —
`f1.bronze.results` (raw strings) vs `f1.silver.results` (typed & trusted).

## 3. What gets deployed

1. **Data** — the 14 Ergast CSVs, uploaded once into the `landing` volume (re-uploaded only if the
   snapshot is refreshed; the snapshot date is recorded in the Bronze spec).
2. **Layer logic** — PySpark / DLT transformation code in `src/` (Bronze, Silver, Gold).
3. **One Lakeflow Declarative Pipeline (DLT)** — wires Bronze → Silver → Gold with expectations and
   quarantine tables. (Free Edition allows 1 active pipeline per type.)
4. **SQL dashboard** — queries in `sql/`, published as a Databricks SQL dashboard on the Gold tables.
5. **Governance metadata** — catalog/schema/table comments and tags applied via Unity Catalog.

## 4. How — deployment runbook

Code lives in **GitHub** (source of truth) and is brought into Databricks via **Git folders**
(Databricks Repos), so the workspace mirrors the repo — no copy-paste drift. Data travels the only
route Free Edition allows: a **UI upload** into a Unity Catalog volume.

```
GitHub (this repo)  ──git──▶  Databricks Git folder  ──▶  DLT pipeline + notebooks + SQL
      ▲                                                          │
      └──────────────── commit specs & code ◀────────────────────┘

Local ~/Downloads/F1/*.csv  ──UI upload──▶  /Volumes/f1/landing/ergast_csv/
```

**Steps:**
1. Create the Databricks Free Edition workspace (one-time).
2. In the workspace: **Git folders → clone** `https://github.com/ShehriyarAhmed1/medallion-lakehouse-databricks`.
3. Create the Unity Catalog objects: catalog `f1` + schemas `landing / bronze / silver / gold / quarantine`,
   and volume `f1.landing.ergast_csv`.
4. **Upload the 14 CSVs** from the local machine into the volume (Catalog Explorer → volume → Upload).
5. Create a **DLT pipeline** pointing at the pipeline source in `src/`; set target catalog/schema.
6. **Run** the pipeline (triggered/manual). Verify expectations & quarantine counts against the specs.
7. Attach the pre-created **serverless SQL warehouse** and build the dashboard from `sql/`.
8. Commit any changes back to GitHub (specs updated, screenshots into `docs/`).

> **Note on Databricks Asset Bundles (DAB):** the "proper" CI/CD way to deploy pipelines/jobs as code.
> We keep the repo DAB-ready (declarative pipeline definition in `src/`) but, given a single Free Edition
> workspace and a tight timeline, deploy via Git folders + the UI for v1. DAB/CI is listed as future work.

## 5. Environments

Free Edition gives **one** workspace, so we run a **single environment**. We simulate separation with
**schemas** and Delta features rather than multiple workspaces:

| Real-world | This project (Free Edition) |
|------------|-----------------------------|
| dev / staging / prod workspaces | one workspace |
| prod catalog vs dev catalog | one `f1` catalog, layered schemas |
| blue/green deploys | DLT **full refresh** re-materializes tables |

## 6. Orchestration & scheduling

- v1: DLT pipeline run **manually / triggered** (keeps compute usage deliberate).
- The source is a static snapshot, so there is no schedule to keep — a re-run only matters after a
  code change or a refreshed CSV upload.

## 7. Cost & quota management (free-tier discipline)

- Use the smallest compute (serverless defaults; 2X-Small SQL warehouse).
- **Never leave compute idling** — stop the warehouse when not querying.
- Watch the daily quota; if compute cuts off, data & settings survive — resume next day.
- The full dataset is ~28 MB of CSV / ~1.0M rows — trivially inside storage limits; the only quota to
  respect is compute time.
- **Everything is pushed to GitHub** so nothing is trapped if the workspace compute is unavailable.

## 8. Reproducibility, rollback & recovery

- **Reproducible:** a clean DLT full-refresh rebuilds identical Gold from the same landed CSVs; the
  CSV snapshot itself is immutable in the volume.
- **Rollback:** Delta **time travel** (`VERSION AS OF`) to inspect/restore prior table states.
- **Recovery:** code + specs in GitHub; the CSVs remain on the local machine (and in the volume);
  workspace objects can be re-created from the runbook above.

## 9. Security & governance

- Unity Catalog owns access control (single-user here; documented grants for the reviewer).
- No secrets in the repo (`.gitignore` covers keys/tokens); the internal project-list PDF is not published.
- The dataset is public domain sports data — no PII concerns beyond public figures' names/DOBs.

## 10. Future / production hardening (out of scope for v1)

- Databricks Asset Bundles + GitHub Actions for CI/CD deploys.
- Separate dev/prod catalogs; automated pipeline scheduling & alerting.
- Incremental ingestion (Auto Loader on the volume) for refreshed snapshots — see project #11.
