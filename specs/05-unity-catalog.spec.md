# M05 — Unity Catalog Governance Spec

| Field | Value |
|-------|-------|
| **Milestone** | M05 |
| **Status** | ✅ Completed |
| **Owner** | Shehriyar Ahmed |
| **Created** | 2026-07-16 |
| **Completed** | 2026-07-16 |
| **Depends on** | M1–M4 (all objects exist) |

---

## 1. Objective

Make the lakehouse **self-describing and access-governed**: after this milestone, a stranger opening
Catalog Explorer can understand every object without reading a single line of code — the catalog,
every schema, every table, and **every column of the trusted layers** carries a comment; objects are
tagged (layer / path / milestone); and the reviewer's read-only access model is documented and applied.

## 2. Scope

**In scope:** comments + tags on catalog `f1`, the landing volume, all 6 schemas, and the 46
notebook-prototype tables (`bronze` 14 · `silver` 14 · `quarantine` 14 · `gold` 4); **100% column
comments** on `f1.silver.*` and `f1.gold.*` (the layers people query); documented + applied grants;
a best-effort tag pass over the 48 pipeline datasets in `f1.medallion`.

**Out of scope:** row/column-level security, dynamic masking (no PII — public sports data);
the dashboard (M6).

## 3. Governance contract

| Object | Comment | Tags |
|--------|---------|------|
| Catalog `f1` | project one-liner + repo URL | `project`, `owner`, `source`, `data_class=public` |
| Volume `landing.ergast_csv` | snapshot date + provenance | — |
| 6 schemas | the layer's one-line job | `layer=<name>` |
| 46 prototype tables | layer template + per-table description (the overview's table inventory) | `layer`, `path=prototype`, `milestone=m1/m2/m3` |
| `f1.medallion.*` (48) | ✔ already commented **in the pipeline code** (M4 — governance-as-code) | `path=production` attempted; pipeline-owned objects may refuse external ALTERs → count reported, not asserted |
| `f1.silver.*` columns | **all 121** — explicit descriptions for meaning-bearing columns, rule-generated for mechanical ones (PKs, FKs, `*_ref`, `url`, session schedule cols) | — |
| `f1.gold.*` columns | **all** mart columns | — |

**Column-comment rules (why generation is safe):** a PK/FK/url column's meaning *is* mechanical —
"FK → f1.silver.races" is the correct documentation. Meaning-bearing columns (e.g. `position` =
*"final classified position — NULL means not classified (DNF/DNS)"*, `milliseconds` = *"numeric
truth; the time string is display-only"*) get hand-written text. The notebook **fails loudly if any
column ends up uncovered** — 100% is asserted, not aspired to.

**Access model (documented for the reviewer):**
| Principal | Access | Why |
|-----------|--------|-----|
| Owner (Shehriyar) | ALL | single-owner project |
| `account users` (reviewer stand-in) | `USE CATALOG` + `USE SCHEMA`/`SELECT` on **gold** and **medallion** only | consumers read serving layers; raw/quarantine stay owner-only |

Grant statements are applied where the group exists (Free Edition) and shown via `SHOW GRANTS`.

## 4. Acceptance criteria

- [x] Catalog, volume, and 6/6 schemas carry comments; catalog + schemas tagged.
- [x] 46/46 prototype tables commented + tagged (`layer`, `path`, `milestone`).
- [x] **0 uncommented columns** in `f1.silver.*` and `f1.gold.*` (asserted in-notebook).
- [x] Grants applied per §3 and visible in `SHOW GRANTS`.
- [x] Verification verdict cell all-✅ — operator's run reported **6/6**.
- [x] **Catalog Explorer spot-check by the operator** — the milestone's visible surface.
- [x] Idempotent re-run; code committed; documented for a learner.

## 5. Hands-on run & verification (operator runbook)

| # | You do | You should see |
|---|--------|----------------|
| 1 | Pull `main`; open `src/governance/05_governance`; attach serverless | Teaching cells |
| 2 | Run cell by cell (comments → tags → columns → grants) | Progress prints per object group |
| 3 | Run the **verdict cell** | Counts vs expected, all ✅ (46 tables, 0 uncommented columns, ≥6 schema tags…) |
| 4 | **Catalog Explorer → f1** (the frontend) | Catalog description + tag chips on the catalog page |
| 5 | → `silver` → `results` → **Columns** tab | Every column with a human-readable description; `position` explains NULL = DNF |
| 6 | → `gold` → any mart · → `medallion` → any dataset | Prototype marts described + tagged; pipeline datasets showing their code-declared comments |
| 7 | → `f1` → **Permissions** tab (or the `SHOW GRANTS` output) | `account users` with read access on gold/medallion only |
| 8 | Re-run the notebook | Identical verdict (idempotent — comments/tags overwrite in place) |
| 9 | Report the verdict | Completion filled + committed |

## 6. Constitution compliance

- **II (contracts):** this spec defines the metadata contract before it's applied. ✅
- **IV (Delta + UC):** governance lives *in* Unity Catalog — comments/tags/grants on UC securables. ✅
- **VI (free tier):** metadata-only operations; one short session. ✅
- **Workflow (v1.2.0):** runbook §5; Catalog Explorer is the visible surface. ✅

---

## ✅ Completion
- **Completed on:** 2026-07-16 — operator's hands-on run (constitution v1.2.0), verdict **6/6 ✅**:
  *"the catalog now documents itself."*
- **What was built:** governance metadata via
  [`src/governance/05_governance.py`](../src/governance/05_governance.py) — catalog + volume + 6
  schema comments/tags, 46 prototype tables commented + tagged (138 tag rows), **every column of
  `f1.silver.*` and `f1.gold.*` commented (0 uncovered, asserted)**, reviewer grants applied
  (`account users` → read on gold + medallion only), best-effort `path=production` tags on the
  pipeline datasets (counts reported in-notebook; their comments come from pipeline code — M4's
  governance-as-code).
- **Acceptance criteria:** all met per the in-notebook verdict (6/6 information_schema checks).
- **Coverage counts:** 1 catalog · 1 volume · 6 schemas · 46 tables · 138 table-tag rows ·
  silver+gold columns 100%.
- **Deviations from spec & why:** none.
- **Commit(s):** `e6a039b` spec · `1ce3011` implementation · this commit (completion).

## Changelog
| Date | Change |
|------|--------|
| 2026-07-16 | Spec drafted; implementation built same day |
| 2026-07-16 | ✅ Completed — operator's run: 6/6 verdict, full column coverage |
