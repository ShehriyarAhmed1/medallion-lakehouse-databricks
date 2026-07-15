# Medallion Lakehouse Constitution

> The non-negotiable principles that govern this project. Every spec, plan, and implementation choice
> must comply. This file is canonical (a copy of Spec Kit's constitution concept, surfaced here for
> visibility). Amendments are versioned (see Governance).

## Core Principles

### I. Medallion layering is sacred (NON-NEGOTIABLE)
Bronze is raw and **immutable** (ingest only — no edits beyond ingestion metadata). Silver is
**cleaned, de-duplicated, schema-enforced, and correctly typed**. Gold is **business-level aggregates**.
Data flows **Bronze → Silver → Gold only** — never backward, never skipping a layer.
*Rationale:* clear lineage, reproducibility, and a trustworthy single source of truth.

### II. Contracts before code (schema-first)
Every table's contract — column names, types, nullability, and keys — is written in that milestone's
spec **before** the table is built. The declared schema is the contract.
*Rationale:* spec-driven development; prevents silent schema drift.

### III. Data quality is enforced, not hoped for (NON-NEGOTIABLE)
Every Silver and Gold table declares **DLT expectations** on its critical columns. Violating rows are
**quarantined or dropped with a logged count** — never silently passed downstream.
*Rationale:* expectations are the executable form of the data contract.

### IV. Delta + Unity Catalog, always
All tables are **Delta** format, addressed by the full `catalog.schema.table` name under **Unity
Catalog**. No loose files as final outputs.
*Rationale:* ACID transactions, time travel, and governed lineage.

### V. One reproducible pipeline
The entire batch flow is **one Lakeflow Declarative Pipeline**. A clean re-run (full refresh) reproduces
identical Gold results. No manual, un-scripted one-off steps.
*Rationale:* reproducibility + respects Free Edition's "1 active pipeline per type" limit.

### VI. Free-tier discipline
Everything is designed to live inside **Databricks Free Edition** (serverless, 2X-Small warehouse, daily
quota). No idle compute. Every artifact is pushed to Git so nothing is trapped if compute is unavailable.
*Rationale:* the platform's real constraints are part of the design, not an afterthought.

## Technology Constraints

- Platform: Databricks Free Edition. Formats/tools: Delta Lake, Lakeflow DLT, Unity Catalog, PySpark,
  Databricks SQL.
- Data source: the Formula 1 (Ergast-schema) CSV snapshot, **uploaded from the local machine** into a
  Unity Catalog volume (outbound internet is restricted — no live downloads or APIs).
- No paid features, no external services, no data leaving the workspace.

## Development Workflow

- **Spec-driven order:** plan (overview + masterplan + this constitution) → per-milestone spec →
  implement → verify → mark complete.
- **One milestone = one spec = one (or few) commit(s)** with a **verbose** message describing what and why.
- **Documented for a learner:** every non-trivial transformation explains its *why* in comments.
- **Hands-on & visible (v1.2.0):** the operator executes every workspace step personally — uploads,
  catalog creation, notebook runs, pipeline runs. Implementations are **notebooks designed for
  cell-by-cell runs** with visible output (`display()`, verdict tables) after every meaningful step —
  never opaque scripts. Every milestone spec includes an **operator runbook** (numbered UI steps +
  what should be visible after each), and every milestone ends on a **visible surface** — notebook
  output, the DLT pipeline graph, Catalog Explorer, or a dashboard. No invisible, code-only milestones.
- **Acceptance gate:** a milestone is "done" only when its spec's acceptance criteria are verified
  **by the operator's own run**; the spec's **Completion** section is then filled in and committed.

## Governance

This constitution supersedes ad-hoc convenience choices. Amendments must be recorded here with a version
bump using semantic versioning:
- **MAJOR** — a principle is removed or redefined incompatibly.
- **MINOR** — a new principle/section is added or materially expanded.
- **PATCH** — clarifications and wording fixes.

Any deviation from a principle in a spec or implementation must be explicitly justified in that
milestone's spec.

**Amendment history:**
- **1.2.0 (2026-07-15)** — Development Workflow: added the **Hands-on & visible** rule (operator runs
  every workspace step personally; notebooks with cell-by-cell visible output; every milestone defines
  an operator runbook and ends on a visible surface). Requested by the owner before M1.
- **1.1.0 (2026-07-15)** — Technology Constraints: data source changed from the built-in NYC Taxi
  `samples` dataset to the Formula 1 (Ergast-schema) CSV snapshot uploaded into a Unity Catalog volume.
  Project restarted on the new dataset; the six core principles are unchanged.

**Version:** 1.2.0 | **Ratified:** 2026-07-08 | **Last Amended:** 2026-07-15
