# Databricks notebook source
# MAGIC %md
# MAGIC # M5 — Unity Catalog governance: comments, tags, grants
# MAGIC
# MAGIC **Spec:** [`specs/05-unity-catalog.spec.md`](https://github.com/ShehriyarAhmed1/medallion-lakehouse-databricks/blob/main/specs/05-unity-catalog.spec.md)
# MAGIC · run **cell by cell**.
# MAGIC
# MAGIC The data is correct (M1–M4 proved it). But it's **mute**: click `silver.results` in Catalog
# MAGIC Explorer today and nothing tells you what it is, where it came from, or what `position = NULL`
# MAGIC means. Governance fixes that with three tools:
# MAGIC
# MAGIC | Tool | What it answers |
# MAGIC |------|-----------------|
# MAGIC | **Comments** | *what is this?* — on catalog, schemas, tables, and every trusted column |
# MAGIC | **Tags** | *how do I filter/find things?* — `layer`, `path=prototype/production`, `milestone` |
# MAGIC | **Grants** | *who may see what?* — reviewers read the serving layers; raw stays owner-only |
# MAGIC
# MAGIC Everything here is **metadata-only** (no data moves) and **idempotent** (re-running overwrites
# MAGIC the same comments/tags in place). The gate at the end: **0 uncommented columns** in silver & gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The metadata contract — spec §3 as code
# MAGIC
# MAGIC Per-table descriptions (the overview's inventory), explicit column descriptions for
# MAGIC meaning-bearing columns, and generation rules for mechanical ones (PK/FK/`*_ref`/`url`/session
# MAGIC columns — their meaning IS mechanical, so generated text is the *correct* documentation).

# COMMAND ----------

CATALOG_COMMENT = (
    "F1 Medallion Lakehouse — Ergast-schema Formula 1 data (1950 → 2026-in-progress, snapshot "
    "2026-07-05) through Bronze → Silver → Gold with quarantine. Spec-first build; source of truth: "
    "https://github.com/ShehriyarAhmed1/medallion-lakehouse-databricks"
)

SCHEMA_COMMENTS = {
    "landing": "Raw file landing zone — the 14 uploaded Ergast CSVs live in the ergast_csv volume. Files, not tables.",
    "bronze": "M1 prototype layer: raw, immutable copies of each CSV (all STRING + ingest provenance). The audit trail.",
    "silver": "M2 prototype layer: typed, deduped, FK-verified single source of truth (snake_case).",
    "quarantine": "M2 prototype layer: rows rejected from silver, kept WITH a _reasons column — nothing silently dropped.",
    "gold": "M3 prototype layer: business marts (joins + aggregates), dashboard-ready.",
    "medallion": "M4 PRODUCTION path: the whole flow rebuilt by the f1-medallion-pipeline (bronze_/silver_/quarantine_/gold_/audit_ datasets).",
}

TABLE_DESC = {
    "races": "One row per Grand Prix, 1950→2026: year, round, circuit, date. The central spine of the model.",
    "results": "One row per driver per race — grid, finishing position, points, laps, race time. The main fact table.",
    "sprint_results": "Sprint-race results (2021+ format), one row per driver per sprint.",
    "qualifying": "Qualifying results — Q1/Q2/Q3 lap-time strings; eliminated drivers have NULL Q2/Q3 by design.",
    "lap_times": "Every lap by every driver since 1996 — the volume table (~874K trusted rows).",
    "pit_stops": "Every pit stop since 1994. duration is a display string (some mm:ss); milliseconds is the numeric truth.",
    "drivers": "Driver dimension — name, date of birth, nationality. Permanent car numbers only exist from 2014.",
    "constructors": "Constructor (team) dimension.",
    "circuits": "Circuit dimension — location, coordinates, altitude.",
    "seasons": "One row per championship season, 1950–2026.",
    "status": "Finishing-status lookup: Finished, Engine, Collision, +1 Lap, …",
    "driver_standings": "OFFICIAL championship standings after each race — may legitimately differ from summed results (disqualifications, deductions).",
    "constructor_standings": "OFFICIAL constructor standings after each race.",
    "constructor_results": "Constructor points per race; status 'D' = disqualified.",
}

GOLD_DESC = {
    "driver_season_summary": "One row per (season, driver): races, wins, podiums, poles, points (as recorded), DNFs, best finish. Answers: who dominated each era?",
    "constructor_season_summary": "One row per (season, constructor): entries, wins, podiums, points, best finish. Answers: how have team fortunes shifted?",
    "pit_stop_evolution": "One row per season since 1994: stop counts and avg/median/fastest durations (seconds). Answers: how have pit stops changed?",
    "circuit_stats": "One row per circuit: races held, first/last season, distinct winners. Answers: what is each track's history?",
}

# natural keys (for "Primary key" comments) and FK-column → dimension map
KEYS = {
    "seasons": ["year"], "status": ["status_id"], "circuits": ["circuit_id"],
    "constructors": ["constructor_id"], "drivers": ["driver_id"], "races": ["race_id"],
    "results": ["result_id"], "sprint_results": ["result_id"], "qualifying": ["qualify_id"],
    "lap_times": ["race_id", "driver_id", "lap"], "pit_stops": ["race_id", "driver_id", "stop"],
    "driver_standings": ["driver_standings_id"], "constructor_standings": ["constructor_standings_id"],
    "constructor_results": ["constructor_results_id"],
}
FK_DIM = {"race_id": "races", "driver_id": "drivers", "constructor_id": "constructors",
          "circuit_id": "circuits", "status_id": "status"}

# meaning-bearing columns — global by name, with (table, column) overrides where meaning shifts
COL_GLOBAL = {
    "name": "Official name.",
    "location": "City / locality.", "country": "Country.",
    "nationality": "Nationality as recorded by Ergast.",
    "lat": "Latitude (WGS84, −90…90).", "lng": "Longitude (WGS84, −180…180).",
    "alt": "Altitude in metres (nullable).",
    "number": "Car number (nullable — permanent numbers only from 2014).",
    "code": "3-letter driver code, e.g. HAM (nullable pre-2004).",
    "forename": "Driver first name.", "surname": "Driver last name.",
    "dob": "Date of birth.",
    "round": "Round number within the season (≥1).",
    "date": "Race date (scheduled races carry future dates).",
    "time": "Time-of-day / duration display string (nullable). Where a milliseconds column exists, THAT is the numeric truth.",
    "grid": "Starting grid slot (nullable; 0 = pit-lane start).",
    "position": "Final classified position. NULL = not classified (DNF/DNS/DSQ) — a meaning, not an error.",
    "position_text": "Position as text: number, or R(etired), D(isqualified), W(ithdrawn), …",
    "position_order": "Always-populated sort key (≥1), even for non-finishers.",
    "points": "Championship points AS RECORDED under that era's rules (10 for a 2008 win, 25 today) — never re-scored.",
    "laps": "Laps completed (≥0).",
    "milliseconds": "Numeric truth for the time/duration, in ms (nullable where never recorded).",
    "fastest_lap": "Lap number of the driver's fastest lap (recorded from 2004).",
    "rank": "Fastest-lap rank within the race (nullable).",
    "fastest_lap_time": "Fastest-lap display string.",
    "fastest_lap_speed": "Fastest-lap average speed, km/h.",
    "lap": "Lap number (≥1).",
    "stop": "Pit-stop sequence number for the driver in the race (≥1).",
    "duration": "Pit-lane duration display string — 764 rows are mm:ss and don't parse as numbers; use milliseconds.",
    "q1": "Q1 best lap string (NULL = no time set).",
    "q2": "Q2 best lap string (NULL = eliminated in Q1).",
    "q3": "Q3 best lap string (NULL = eliminated in Q1/Q2).",
    "wins": "Cumulative season wins at this point (≥0).",
    "status": "Finishing status text.",
    "year": "Season year.",
}
COL_OVERRIDE = {
    ("lap_times", "position"): "Track position DURING that lap (not the final result).",
    ("lap_times", "time"): "Lap-time display string (m:ss.SSS); milliseconds is the numeric truth.",
    ("pit_stops", "time"): "Clock time of day the stop began.",
    ("driver_standings", "points"): "OFFICIAL cumulative championship points after this race.",
    ("constructor_standings", "points"): "OFFICIAL cumulative constructor points after this race.",
    ("constructor_results", "points"): "Constructor points scored in this single race.",
    ("constructor_results", "status"): "Almost always NULL; 'D' = constructor disqualified from the race.",
    ("driver_standings", "position"): "Championship position after this race (nullable).",
    ("constructor_standings", "position"): "Constructor championship position after this race (nullable).",
}
GOLD_COL = {
    "season": "Season year.", "driver": "Driver full name.", "constructor": "Constructor name.",
    "races_entered": "Result rows for this driver-season (one per race entered).",
    "entries": "Car-entries (a 2-car team logs 2 per race).",
    "podiums": "Finishes in the top 3 (DNFs never count — NULL-safe).",
    "poles": "Starts from grid slot 1 (grid, not qualifying — penalties reshuffle).",
    "dnfs": "Entries with no classified position.",
    "best_finish": "Best classified position in the season (NULL if never classified).",
    "stops": "Pit stops that season.",
    "avg_stop_s": "Mean pit-lane duration, seconds.", "median_stop_s": "Median pit-lane duration, seconds.",
    "fastest_stop_s": "Fastest pit-lane duration, seconds.",
    "circuit": "Circuit name.", "races_held": "Grands Prix hosted (incl. scheduled).",
    "first_season": "First season raced here.", "last_season": "Most recent (or scheduled) season.",
    "distinct_winners": "Distinct race winners at this circuit (0 = scheduled-only so far).",
    "wins": "Race wins.", "points": "Points as recorded (never re-scored).",
    "nationality": "Nationality.", "code": "3-letter driver code (nullable).",
}

print(f"contract loaded: {len(TABLE_DESC)} tables · {len(COL_GLOBAL)}+{len(COL_OVERRIDE)} column descriptions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Catalog, volume & schema comments + tags

# COMMAND ----------

def esc(text):
    return text.replace("'", "\\'")

spark.sql(f"COMMENT ON CATALOG f1 IS '{esc(CATALOG_COMMENT)}'")
spark.sql("ALTER CATALOG f1 SET TAGS ('project' = 'medallion-lakehouse', 'owner' = 'shehriyar', "
          "'source' = 'ergast-f1', 'data_class' = 'public')")
spark.sql("COMMENT ON VOLUME f1.landing.ergast_csv IS "
          "'The 14 Ergast F1 CSVs, uploaded 2026-07-15 (snapshot dated 2026-07-05). The ultimate source — never edited.'")

for schema, comment in SCHEMA_COMMENTS.items():
    spark.sql(f"COMMENT ON SCHEMA f1.{schema} IS '{esc(comment)}'")
    spark.sql(f"ALTER SCHEMA f1.{schema} SET TAGS ('layer' = '{schema}')")
    print(f"schema f1.{schema} ✅")

print("catalog + volume + 6 schemas commented & tagged")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — The 46 prototype tables: comments + tags
# MAGIC
# MAGIC Layer template + the per-table description. Tags make the layers filterable in Catalog
# MAGIC Explorer's search: `layer`, `path=prototype` (vs the pipeline's production path), `milestone`.

# COMMAND ----------

def govern_table(fqn, comment, tags):
    spark.sql(f"COMMENT ON TABLE {fqn} IS '{esc(comment)}'")
    tag_sql = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {fqn} SET TAGS ({tag_sql})")

count = 0
for t, desc in TABLE_DESC.items():
    govern_table(f"f1.bronze.{t}",
                 f"Bronze (M1 prototype): raw as-ingested copy of {t}.csv — all STRING + provenance. {desc}",
                 {"layer": "bronze", "path": "prototype", "milestone": "m1"})
    govern_table(f"f1.silver.{t}",
                 f"Silver (M2 prototype): typed, deduped, FK-verified. {desc}",
                 {"layer": "silver", "path": "prototype", "milestone": "m2"})
    govern_table(f"f1.quarantine.{t}",
                 f"Quarantine (M2): rows rejected from silver.{t}, kept with _reasons — nothing silently dropped.",
                 {"layer": "quarantine", "path": "prototype", "milestone": "m2"})
    count += 3
    print(f"{t:<24} bronze/silver/quarantine ✅")

for mart, desc in GOLD_DESC.items():
    govern_table(f"f1.gold.{mart}", f"Gold (M3 prototype): {desc}",
                 {"layer": "gold", "path": "prototype", "milestone": "m3"})
    count += 1

print(f"\n{count} prototype tables commented & tagged (expected 46)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Every column of silver & gold gets a comment
# MAGIC
# MAGIC Rule chain per column: natural key → FK → `*_ref` → `url` → session-schedule → explicit
# MAGIC description. If a column matches nothing, the cell **fails and names it** — coverage is
# MAGIC asserted, not aspired to.

# COMMAND ----------

def describe_column(table, col):
    if (table, col) in COL_OVERRIDE:
        return COL_OVERRIDE[(table, col)]
    if col in KEYS.get(table, []):
        extra = " (part of the natural key)" if len(KEYS[table]) > 1 else " (primary key)"
        base = COL_GLOBAL.get(col) or (f"FK → f1.silver.{FK_DIM[col]}" if col in FK_DIM else "Identifier.")
        return base.rstrip(".") + extra + "."
    if col in FK_DIM and table != FK_DIM[col]:
        return f"FK → f1.silver.{FK_DIM[col]}."
    if col == "year" and table == "races":
        return "Season year — FK → f1.silver.seasons."
    if col.endswith("_ref"):
        return "Stable text identifier from Ergast (join-friendly slug)."
    if col == "url":
        return "Source Wikipedia URL (provenance)."
    if col.startswith(("fp1", "fp2", "fp3", "quali_", "sprint_")):
        return "Weekend session schedule (nullable — recorded for the modern era only)."
    if col == "_reasons":
        return "Quality-gate column: constant '' on trusted rows (see spec 04 §3.3)."
    return COL_GLOBAL.get(col)

missing, applied = [], 0
for t in TABLE_DESC:
    for col in spark.table(f"f1.silver.{t}").columns:
        d = describe_column(t, col)
        if d is None:
            missing.append(f"silver.{t}.{col}")
            continue
        spark.sql(f"ALTER TABLE f1.silver.{t} ALTER COLUMN {col} COMMENT '{esc(d)}'")
        applied += 1
    print(f"silver.{t} columns ✅")

for mart in GOLD_DESC:
    for col in spark.table(f"f1.gold.{mart}").columns:
        d = GOLD_COL.get(col) or describe_column(mart, col)
        if d is None:
            missing.append(f"gold.{mart}.{col}")
            continue
        spark.sql(f"ALTER TABLE f1.gold.{mart} ALTER COLUMN {col} COMMENT '{esc(d)}'")
        applied += 1
    print(f"gold.{mart} columns ✅")

assert not missing, f"uncovered columns — add descriptions for: {missing}"
print(f"\n{applied} column comments applied · 0 uncovered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Production path (f1.medallion): best-effort tags
# MAGIC
# MAGIC The 48 pipeline datasets already carry **comments from the pipeline code** (M4 —
# MAGIC governance-as-code). Pipeline-owned objects may refuse external ALTERs, so tagging is
# MAGIC attempted and *reported*, never asserted.

# COMMAND ----------

med_tables = [r.tableName for r in spark.sql("SHOW TABLES IN f1.medallion").collect()]
ok, blocked = 0, 0
for t in med_tables:
    try:
        spark.sql(f"ALTER TABLE f1.medallion.{t} SET TAGS ('path' = 'production', 'milestone' = 'm4')")
        ok += 1
    except Exception:
        blocked += 1
print(f"medallion datasets: {len(med_tables)} found · tagged {ok} · refused {blocked} (pipeline-owned — fine, reported not asserted)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Grants: the reviewer access model (spec §3)
# MAGIC
# MAGIC Consumers read the **serving layers** (gold + medallion); raw/quarantine stay owner-only.
# MAGIC `account users` is Free Edition's stand-in for "everyone else".

# COMMAND ----------

GRANTS = [
    "GRANT USE CATALOG ON CATALOG f1 TO `account users`",
    "GRANT USE SCHEMA ON SCHEMA f1.gold TO `account users`",
    "GRANT SELECT ON SCHEMA f1.gold TO `account users`",
    "GRANT USE SCHEMA ON SCHEMA f1.medallion TO `account users`",
    "GRANT SELECT ON SCHEMA f1.medallion TO `account users`",
]
for g in GRANTS:
    try:
        spark.sql(g)
        print(f"✅ {g}")
    except Exception as e:
        print(f"⚠️ skipped ({type(e).__name__}): {g}")

display(spark.sql("SHOW GRANTS ON SCHEMA f1.gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Verification verdict (from information_schema — the catalog describing itself)

# COMMAND ----------

from pyspark.sql import functions as F

def one(q):
    return spark.sql(q).first()[0]

our_schemas = "'" + "','".join(SCHEMA_COMMENTS) + "'"
checks = [
    ("schemas commented", 6, one(
        f"SELECT count(*) FROM f1.information_schema.schemata WHERE schema_name IN ({our_schemas}) AND comment IS NOT NULL")),
    ("prototype tables commented", 46, one(
        "SELECT count(*) FROM f1.information_schema.tables WHERE table_schema IN ('bronze','silver','quarantine','gold') AND comment IS NOT NULL")),
    ("silver columns WITHOUT comment", 0, one(
        "SELECT count(*) FROM f1.information_schema.columns WHERE table_schema = 'silver' AND comment IS NULL")),
    ("gold columns WITHOUT comment", 0, one(
        "SELECT count(*) FROM f1.information_schema.columns WHERE table_schema = 'gold' AND comment IS NULL")),
    ("schema tags", 6, one(
        f"SELECT count(*) FROM f1.information_schema.schema_tags WHERE schema_name IN ({our_schemas}) AND tag_name = 'layer'")),
    ("prototype table tag rows (3 × 46)", 138, one(
        "SELECT count(*) FROM f1.information_schema.table_tags WHERE schema_name IN ('bronze','silver','quarantine','gold')")),
]
verdict = [(n, str(e), str(a), "✅" if e == a else "❌") for n, e, a in checks]
display(spark.createDataFrame(verdict, "check string, expected string, actual string, ok string"))

passed = sum(1 for *_, ok in verdict if ok == "✅")
assert passed == len(checks), f"only {passed}/{len(checks)} governance checks passed"
print(f"M5 governance verified: {passed}/{len(checks)} ✅ — the catalog now documents itself")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The frontend — go LOOK at it
# MAGIC
# MAGIC **Catalog Explorer → f1**: the catalog page shows the project description + tag chips.
# MAGIC → **silver → results → Columns tab**: every column explains itself (`position` tells you
# MAGIC NULL = DNF). → **f1 → Permissions**: `account users` reads gold/medallion only.
# MAGIC
# MAGIC A stranger can now understand this lakehouse without opening a single notebook —
# MAGIC that's what governance means. **Next: M6, the dashboard.**
