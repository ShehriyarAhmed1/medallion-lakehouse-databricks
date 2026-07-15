# MNN — <Milestone Title> Spec

| Field | Value |
|-------|-------|
| **Milestone** | MNN |
| **Status** | Draft ⬜ / Approved / In Progress / ✅ Completed |
| **Owner** | Shehriyar Ahmed |
| **Created** | YYYY-MM-DD |
| **Completed** | — |
| **Depends on** | <previous milestone, or none> |

---

## 1. Objective
<One or two sentences: what this milestone delivers and why it matters.>

## 2. Scope
**In scope:**
- …

**Out of scope:**
- …

## 3. Data contract

**Input**
| Source | Object | Notes |
|--------|--------|-------|
| … | `catalog.schema.table` or samples path | … |

**Output**
| Column | Type | Nullable | Key | Description |
|--------|------|----------|-----|-------------|
| … | … | … | … | … |

- **Table:** `f1.<layer>.<table>` (Delta, Unity Catalog)
- **Write mode:** <append / overwrite / merge / streaming>

## 4. Transformation / logic rules
<The rules this milestone applies — cleaning, dedup keys, casts, filters, aggregations. Explain the WHY
for a learner.>

## 5. Data-quality expectations
| Expectation | Rule | Action on violation |
|-------------|------|---------------------|
| `valid_...` | e.g. `points >= 0` | drop / quarantine / fail |

## 6. Acceptance criteria
- [ ] …
- [ ] Table exists at `f1.<layer>.<table>` with the contracted schema.
- [ ] Expectations wired; quarantine/violation counts logged.
- [ ] Code committed; documented for a learner.

## 7. Deployment notes
<Where/how this runs in the workspace; any Free Edition considerations.>

## 8. Constitution compliance
<Confirm alignment with planning/constitution.md; justify any deviation.>

---

## ✅ Completion  *(fill in when done)*
- **Completed on:** YYYY-MM-DD
- **What was built:** …
- **Acceptance criteria:** all met? (list any exceptions)
- **Actual output schema / row counts:** …
- **Quarantine / DQ results:** …
- **Deviations from spec & why:** …
- **Commit(s):** `<hash> <message>`

## Changelog
| Date | Change |
|------|--------|
| YYYY-MM-DD | Spec drafted |
