# Folder Structure — every folder & file justified

> Instruction #1 ("every folder and file must have a real purpose") and instruction #4 ("for every file:
> why it exists, when it runs, what depends on it, mandatory or optional, what happens if removed").
> If a reviewer points at *any* path, this is the answer.

---

## 1. Annotated tree

```
medallion-lakehouse-databricks/
├── README.md                      # public front door (what/why + milestone status)
├── .gitignore                     # keeps secrets & the internal PDF out of Git
│
├── planning/                      # M0 — decisions made BEFORE any code
│   ├── overview.md                #   the project: goal, dataset, stack, milestones
│   ├── masterplan.md              #   deployment strategy: who / where / how
│   └── constitution.md            #   6 non-negotiable principles every spec obeys
│
├── specs/                         # one spec per milestone, written spec-first
│   ├── README.md                  #   spec lifecycle + commit conventions
│   ├── _TEMPLATE.spec.md          #   the template every spec copies
│   ├── 01-bronze.spec.md          #   M1 spec (+ Completion)
│   ├── 02-silver.spec.md          #   M2 spec (+ Completion)
│   ├── 03-gold.spec.md            #   M3 spec (+ Completion)
│   └── 04-dlt-pipeline.spec.md    #   M4 spec (draft → completed after the run)
│
├── src/                           # implementation
│   ├── bronze/01_bronze_ingest.py #   M1 notebook — raw ingest
│   ├── silver/02_silver_clean.py  #   M2 notebook — clean/dedupe/quarantine
│   ├── gold/03_gold_marts.py      #   M3 notebook — 3 aggregated marts
│   └── pipelines/                 #   M4 — the one production DLT pipeline
│       └── medallion_pipeline.py  #     re-expresses M1–M3 as one declarative pipeline
│
├── sql/                           # M6 — Databricks SQL dashboard queries (added at M6)
├── docs/                          # the diagrams you're reading now
│   ├── architecture.md            #   overall architecture + tech justification
│   ├── data-flow.md               #   row/column flow with real counts
│   ├── pipeline-flow.md           #   the DLT dataset DAG + expectations
│   ├── execution-flow.md          #   run lifecycle, start → finish
│   └── folder-structure.md        #   ← this file
│
├── .specify/                      # Spec Kit engine (constitution tooling) — hidden
├── .claude/skills/                # Spec Kit slash-command definitions — hidden
└── Updated_Project_List.pdf       # internal brief — git-IGNORED, never published
```

> `sql/` is shown because the README/overview reference it, but it is **created at M6**, not before —
> we don't scaffold empty folders (instruction #1). The same applies to `specs/05..07` (future milestones).

---

## 2. File-by-file (why · when · depends · mandatory? · if removed)

### Root
| File | Why it exists | When it runs | Depended on by | Mandatory? | If removed |
|------|---------------|--------------|----------------|:----------:|------------|
| `README.md` | Front door: what the project is, the stack, milestone status | Read by humans/GitHub | Reviewers | ⭐ Recommended | Repo still works; loses its first impression |
| `.gitignore` | Keeps secrets/tokens and the internal PDF out of version control | On every `git add` | Git | ✅ Yes | Risk of committing secrets / the private PDF |

### `planning/` — the "why", decided before code (M0)
| File | Why it exists | When it runs | Depended on by | Mandatory? | If removed |
|------|---------------|--------------|----------------|:----------:|------------|
| `overview.md` | Single source for goal, dataset, stack, milestone map | Read at project start / demo | All specs | ⭐ Recommended | Lose the "big picture"; specs lose their anchor |
| `masterplan.md` | Deployment strategy (who/where/how), cost & recovery | Read before deploying | The runbook | ⭐ Recommended | No documented path from repo → running lakehouse |
| `constitution.md` | The 6 non-negotiable principles | Every spec must comply | Every spec | ✅ Yes (governance) | Decisions lose their objective yardstick |

### `specs/` — one spec per milestone (spec-first)
| File | Why it exists | When it runs | Depended on by | Mandatory? | If removed |
|------|---------------|--------------|----------------|:----------:|------------|
| `README.md` | Spec lifecycle + commit conventions | Read when starting a milestone | Contributors | ⭐ Recommended | Process becomes tribal knowledge |
| `_TEMPLATE.spec.md` | Consistent shape for every spec | Copied to start a new spec | New specs | ⭐ Recommended | Specs drift in structure |
| `01..04-*.spec.md` | The contract for each milestone, written *before* code | Written pre-build; Completion filled post-verify | The matching `src/` code | ✅ Yes (per constitution II) | You lose the contract & the "what/why" record for that layer |

### `src/` — the implementation
| File | Why it exists | When it runs | Depends on | Mandatory? | If removed |
|------|---------------|--------------|------------|:----------:|------------|
| `bronze/01_bronze_ingest.py` | M1: raw ingest + provenance | Mode A, step 1 | `samples.nyctaxi.trips` | ✅ (dev record) | Lose the Bronze prototype & its verification |
| `silver/02_silver_clean.py` | M2: dedupe, quarantine split, derive, schema-enforce | Mode A, step 2 | `bronze.trips_raw` | ✅ (dev record) | Lose the Silver prototype (the trickiest logic) |
| `gold/03_gold_marts.py` | M3: 3 business marts | Mode A, step 3 | `silver.trips_clean` | ✅ (dev record) | Lose the Gold prototype |
| `pipelines/medallion_pipeline.py` | M4: the **one** production pipeline (all layers declaratively) | Mode B (DLT run) | `samples.nyctaxi.trips` | ✅ (the production artifact) | No reproducible production pipeline — Constitution V unmet |

> **Why keep both the notebooks *and* the pipeline?** The notebooks are the **development/prototype record**
> (how each layer was worked out and verified, with printed checks). The pipeline is the **production
> artifact** (one reproducible run). They aren't duplication-for-its-own-sake — they play different roles,
> and the M4 spec explicitly scopes the notebooks as the prototype record. (If a reviewer pushes on this,
> the honest answer is: in a pure-production repo you might drop the notebooks once the pipeline is trusted;
> here they're kept as the learning/verification trail this project is built to show.)

### Hidden tooling — `.specify/` and `.claude/`
| Path | Why it exists | Mandatory? | If removed |
|------|---------------|:----------:|------------|
| `.specify/` | GitHub **Spec Kit** engine — provides the constitution concept & spec scaffolding scripts | Optional (tooling) | Spec-first *tooling* gone; the written specs/constitution still stand on their own |
| `.claude/skills/` | Spec Kit slash-command definitions used while authoring specs | Optional (tooling) | Lose the authoring shortcuts; no effect on the data pipeline |

> These are **developer tooling, not runtime**. They don't touch the data or run in Databricks. Being able
> to say *"that's authoring tooling, the pipeline doesn't depend on it"* is exactly the file-level clarity
> instruction #4 asks for.

### Ignored / not part of the deliverable
| Path | Status | Why |
|------|--------|-----|
| `Updated_Project_List.pdf` | **git-ignored** | Internal brief; masterplan §9 says it must not be published |
| `Medallion Lakehouse.code-workspace` | untracked, optional | A VS Code workspace file. It originally listed `.` twice (a bug that showed the folder duplicated); now a single valid folder. Purely a local editor convenience — safe to delete, not part of the pipeline. |

---

## 3. Reviewer questions this answers

- *"Is there any file you can't explain?"* → No — every path is in the tables above.
- *"Any empty/placeholder folders?"* → No. `sql/` and future specs appear only when their milestone starts.
- *"What's mandatory vs. optional?"* → constitution/specs/`src` are mandatory; `.specify`/`.claude` are optional tooling; the PDF is ignored.
- *"What breaks if I delete X?"* → the "If removed" column for every file.
