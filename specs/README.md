# Specs — one spec per milestone

This folder holds a **spec file for every milestone**. Specs are written **before** implementation
(spec-first) and updated with a **Completion** section when the milestone is done.

## Naming

```
NN-<milestone>.spec.md      e.g. 01-bronze.spec.md, 04-dlt-pipeline.spec.md
_TEMPLATE.spec.md           the template every spec copies
```

`NN` is a zero-padded order number. Names are lowercase, hyphen-separated.

## Milestone index

| # | Milestone | Spec file | Status |
|---|-----------|-----------|--------|
| M1 | Bronze — CSV upload + raw ingestion | `01-bronze.spec.md` | ✅ Completed |
| M2 | Silver — type / clean / conform / dedupe | `02-silver.spec.md` | ✅ Completed |
| M3 | Gold — aggregated marts | `03-gold.spec.md` | ⬜ Not started |
| M4 | DLT pipeline + expectations | `04-dlt-pipeline.spec.md` | ⬜ Not started |
| M5 | Unity Catalog governance | `05-unity-catalog.spec.md` | ⬜ Not started |
| M6 | Databricks SQL dashboard | `06-sql-dashboard.spec.md` | ⬜ Not started |
| M7 | Portfolio packaging | `07-packaging.spec.md` | ⬜ Not started |

## Spec lifecycle

```
Draft  ──▶  Approved  ──▶  In Progress  ──▶  ✅ Completed
```

- **Draft** — spec written from `_TEMPLATE.spec.md`, committed.
- **Approved** — reviewed; ready to build.
- **In Progress** — implementation underway.
- **Completed** — acceptance criteria verified; the spec's **Completion** section is filled in.

## Commit conventions (per manager's requirement)

- **One milestone = one spec = its own commit(s)** with a **verbose** message (what changed *and why*).
- Suggested format:
  ```
  <type>(m<NN>-<milestone>): <short summary>

  - what was specified / built
  - why key decisions were made
  - acceptance criteria status
  ```
  Types: `spec` (spec authored/updated), `feat` (implementation), `docs`, `chore`.
- When a milestone is **completed**, fill the spec's **Completion** section and commit
  (e.g. `docs(m01-bronze): mark milestone complete — acceptance criteria verified`).

## How to start a new milestone spec

1. Copy `_TEMPLATE.spec.md` → `NN-<milestone>.spec.md`.
2. Fill every section; leave `Completion` empty until done.
3. Commit the draft, implement, then complete and commit.
