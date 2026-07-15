# Databricks notebook source
# MAGIC %md
# MAGIC # M3 — Gold: business marts (joins, aggregates, and the first charts)
# MAGIC
# MAGIC **Spec:** [`specs/03-gold.spec.md`](https://github.com/ShehriyarAhmed1/medallion-lakehouse-databricks/blob/main/specs/03-gold.spec.md)
# MAGIC · run **cell by cell**.
# MAGIC
# MAGIC Silver is trustworthy but *normalized* — answering "how many wins does Hamilton have?" needs
# MAGIC joins and a scan every single time. **Gold does that work once** and stores the answers in four
# MAGIC small marts, each built for one business question:
# MAGIC
# MAGIC | Mart | Question |
# MAGIC |------|----------|
# MAGIC | `driver_season_summary` | who dominated each season/era? |
# MAGIC | `constructor_season_summary` | how have team fortunes shifted? |
# MAGIC | `pit_stop_evolution` | how have pit stops changed since 1994? |
# MAGIC | `circuit_stats` | what's the history of each track? |
# MAGIC
# MAGIC The gate: every mart must **reconcile back to Silver** against golden numbers pre-computed from
# MAGIC the source (spec §5). Gold never fixes data — if a number is wrong here, the fix belongs in Silver.

# COMMAND ----------

from pyspark.sql import functions as F

# golden numbers pre-computed from the source CSVs (spec §5) — the run must reproduce them
GOLDEN = {
    "driver_rows": 3_254, "constructor_rows": 1_132, "pit_rows": 33, "circuit_rows": 78,
    "entries": 27_436, "points": 56_520.1,
    "wins": 1_161, "podiums": 3_495, "poles": 1_168, "dnfs": 10_953,
    "stops": 22_475, "races_held": 1_171,
}

results = spark.table("f1.silver.results")
races = spark.table("f1.silver.races")
drivers = spark.table("f1.silver.drivers")
constructors = spark.table("f1.silver.constructors")
circuits = spark.table("f1.silver.circuits")
pit_stops = spark.table("f1.silver.pit_stops")

print("silver inputs loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The first JOIN of the project
# MAGIC
# MAGIC A join glues tables together on a shared key: every `results` row carries a `race_id` — the join
# MAGIC looks up that race's season and name, and the same for driver and team. This is only *safe*
# MAGIC because M2 verified every FK: no result points at a missing race/driver/team, so an inner join
# MAGIC loses nothing. Watch our old friend — results row #1 — gain its context.

# COMMAND ----------

base = (
    results
    .join(races.select("race_id", F.col("year").alias("season"), F.col("name").alias("race")), "race_id")
    .join(drivers.select("driver_id", F.concat_ws(" ", "forename", "surname").alias("driver"),
                         "code", F.col("nationality").alias("driver_nationality")), "driver_id")
    .join(constructors.select("constructor_id", F.col("name").alias("constructor"),
                              F.col("nationality").alias("constructor_nationality")), "constructor_id")
)

assert base.count() == results.count(), "inner joins lost rows — FK integrity broken?"
display(base.filter(F.col("result_id") == 1)
        .select("season", "race", "driver", "constructor", "grid", "position", "points"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 1 — `driver_season_summary`
# MAGIC
# MAGIC One row per driver per season. The metric definitions are the spec's §3 table — note how
# MAGIC `COUNT(CASE WHEN …)` naturally ignores NULLs, so a DNF (position NULL) can never count as a podium.

# COMMAND ----------

driver_season = (
    base.groupBy("season", "driver_id", "driver", "code", F.col("driver_nationality").alias("nationality"))
    .agg(
        F.count("*").alias("races_entered"),
        F.count(F.when(F.col("position") == 1, 1)).alias("wins"),
        F.count(F.when(F.col("position") <= 3, 1)).alias("podiums"),
        F.count(F.when(F.col("grid") == 1, 1)).alias("poles"),
        F.round(F.sum("points"), 1).alias("points"),
        F.count(F.when(F.col("position").isNull(), 1)).alias("dnfs"),
        F.min("position").alias("best_finish"),
    )
)
driver_season.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable("f1.gold.driver_season_summary")

print(f"f1.gold.driver_season_summary · {spark.table('f1.gold.driver_season_summary').count():,} rows")
display(spark.table("f1.gold.driver_season_summary").orderBy(F.desc("points")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 2 — `constructor_season_summary`
# MAGIC
# MAGIC Same idea at team level. `entries` counts *car-entries* (a 2-car team logs 2 per race).

# COMMAND ----------

constructor_season = (
    base.groupBy("season", "constructor_id", "constructor", F.col("constructor_nationality").alias("nationality"))
    .agg(
        F.count("*").alias("entries"),
        F.count(F.when(F.col("position") == 1, 1)).alias("wins"),
        F.count(F.when(F.col("position") <= 3, 1)).alias("podiums"),
        F.round(F.sum("points"), 1).alias("points"),
        F.min("position").alias("best_finish"),
    )
)
constructor_season.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable("f1.gold.constructor_season_summary")

print(f"f1.gold.constructor_season_summary · {spark.table('f1.gold.constructor_season_summary').count():,} rows")
display(spark.table("f1.gold.constructor_season_summary").orderBy(F.desc("points")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 3 — `pit_stop_evolution`
# MAGIC
# MAGIC One row per season since 1994. `stops` counts every stop; the averages use `milliseconds`
# MAGIC (the numeric truth — remember, 764 duration strings are `mm:ss` and don't parse), and the
# MAGIC 3 stops with NULL milliseconds count as stops but stay out of the averages.

# COMMAND ----------

pit_evolution = (
    pit_stops.join(races.select("race_id", F.col("year").alias("season")), "race_id")
    .groupBy("season")
    .agg(
        F.count("*").alias("stops"),
        F.round(F.avg("milliseconds") / 1000, 3).alias("avg_stop_s"),
        F.round(F.expr("percentile(milliseconds, 0.5)") / 1000, 3).alias("median_stop_s"),
        F.round(F.min("milliseconds") / 1000, 3).alias("fastest_stop_s"),
    )
    .orderBy("season")
)
pit_evolution.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable("f1.gold.pit_stop_evolution")

print(f"f1.gold.pit_stop_evolution · {spark.table('f1.gold.pit_stop_evolution').count():,} rows")
display(spark.table("f1.gold.pit_stop_evolution").orderBy("season"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 4 — `circuit_stats`
# MAGIC
# MAGIC One row per circuit. `distinct_winners` joins winners back through races; a circuit whose only
# MAGIC races are still scheduled (2026) has 0 — a LEFT join + fill, not a lost row.

# COMMAND ----------

circuit_agg = races.groupBy("circuit_id").agg(
    F.count("*").alias("races_held"),
    F.min("year").alias("first_season"),
    F.max("year").alias("last_season"),
)
winners = (
    results.filter(F.col("position") == 1)
    .join(races.select("race_id", "circuit_id"), "race_id")
    .groupBy("circuit_id").agg(F.countDistinct("driver_id").alias("distinct_winners"))
)
circuit_stats = (
    circuits.select("circuit_id", F.col("name").alias("circuit"), "location", "country")
    .join(circuit_agg, "circuit_id")            # inner: only circuits that ever hosted a GP
    .join(winners, "circuit_id", "left")        # left: scheduled-only circuits keep their row
    .fillna(0, ["distinct_winners"])
)
circuit_stats.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable("f1.gold.circuit_stats")

print(f"f1.gold.circuit_stats · {spark.table('f1.gold.circuit_stats').count():,} rows")
display(spark.table("f1.gold.circuit_stats").orderBy(F.desc("races_held")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation verdict — spec §5 (the M3 gate)
# MAGIC
# MAGIC Every total must tie back to Silver, and the two season marts must agree **with each other**
# MAGIC (the triangle check: driver mart = constructor mart = silver). All against numbers computed
# MAGIC from the source *before* this notebook existed.
# MAGIC
# MAGIC **Counts match exactly; points match within ±0.5.** Why the tolerance: in the 1950s drivers
# MAGIC *shared cars*, so points were split — halves, thirds, even **sevenths** (the 1954 British GP
# MAGIC fastest-lap point went to seven drivers, 1/7 ≈ 0.142857 each). Rounding each group to 1dp and
# MAGIC then summing therefore gives a slightly different total per grouping path (raw 56,520.05 →
# MAGIC driver path 56,519.8, constructor path 56,520.0). Exact float equality is the wrong test —
# MAGIC a real engineering lesson caught by this very cell's first run.

# COMMAND ----------

ds = spark.table("f1.gold.driver_season_summary")
cs = spark.table("f1.gold.constructor_season_summary")
pe = spark.table("f1.gold.pit_stop_evolution")
ct = spark.table("f1.gold.circuit_stats")

d = ds.agg(F.count("*"), F.sum("races_entered"), F.round(F.sum("points"), 1),
           F.sum("wins"), F.sum("podiums"), F.sum("poles"), F.sum("dnfs")).first()
c = cs.agg(F.count("*"), F.sum("entries"), F.round(F.sum("points"), 1)).first()
p = pe.agg(F.count("*"), F.sum("stops")).first()
t = ct.agg(F.count("*"), F.sum("races_held")).first()

# tolerance 0 = exact (counts can never drift); 0.5 = points (rounding-path-dependent, see above)
checks = [
    ("driver mart rows",              GOLDEN["driver_rows"],      d[0], 0),
    ("Σ races_entered = silver.results", GOLDEN["entries"],       d[1], 0),
    ("Σ points (driver mart)",        GOLDEN["points"],           d[2], 0.5),
    ("Σ wins",                        GOLDEN["wins"],             d[3], 0),
    ("Σ podiums",                     GOLDEN["podiums"],          d[4], 0),
    ("Σ poles",                       GOLDEN["poles"],            d[5], 0),
    ("Σ dnfs",                        GOLDEN["dnfs"],             d[6], 0),
    ("constructor mart rows",         GOLDEN["constructor_rows"], c[0], 0),
    ("Σ entries = silver.results",    GOLDEN["entries"],          c[1], 0),
    ("Σ points (constructor ≈ driver)", d[2],                     c[2], 0.5),
    ("pit mart rows (1994–2026)",     GOLDEN["pit_rows"],         p[0], 0),
    ("Σ stops = silver.pit_stops",    GOLDEN["stops"],            p[1], 0),
    ("circuit mart rows",             GOLDEN["circuit_rows"],     t[0], 0),
    ("Σ races_held = silver.races",   GOLDEN["races_held"],       t[1], 0),
]
verdict = [(name, str(exp), str(act), "exact" if tol == 0 else f"±{tol}",
            "✅" if abs(exp - act) <= tol else "❌") for name, exp, act, tol in checks]
display(spark.createDataFrame(verdict, "check string, expected string, actual string, tolerance string, ok string"))

passed = sum(1 for *_, ok in verdict if ok == "✅")
assert passed == len(checks), f"only {passed}/{len(checks)} reconciliation checks passed"
print(f"M3 Gold verified: {passed}/{len(checks)} reconciliation checks ✅ — every mart ties back to Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The payoff — see 77 seasons of Formula 1
# MAGIC
# MAGIC Three charts straight off the marts. Sanity anchors: Hamilton **106** wins, Schumacher **91**,
# MAGIC Verstappen **71** — if the chart says otherwise, the mart is wrong, not history.

# COMMAND ----------

import matplotlib.pyplot as plt

INK, MUTED, GRID = "#0b0b0b", "#898781", "#e1e0d9"
BLUE, AQUA, YELLOW = "#2a78d6", "#1baf7a", "#eda100"  # validated categorical slots 1–3


def clean_axes(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9)


top10 = (spark.table("f1.gold.driver_season_summary")
         .groupBy("driver").agg(F.sum("wins").alias("wins"))
         .orderBy(F.desc("wins")).limit(10).toPandas().iloc[::-1])

fig, ax = plt.subplots(figsize=(9, 4.5))
bars = ax.barh(top10["driver"], top10["wins"], color=BLUE, height=0.62)
for bar, v in zip(bars, top10["wins"]):
    ax.text(v + 1, bar.get_y() + bar.get_height() / 2, str(v), va="center", fontsize=9, color=INK)
ax.set_title("Most Grand Prix wins, all time (1950–2026)", loc="left", fontsize=12, color=INK, pad=12)
ax.xaxis.grid(True, color=GRID, linewidth=0.8)
ax.set_axisbelow(True)
clean_axes(ax)
plt.tight_layout()
plt.show()

# COMMAND ----------

trio = (spark.table("f1.gold.driver_season_summary")
        .filter(F.col("driver").isin("Lewis Hamilton", "Michael Schumacher", "Max Verstappen"))
        .select("season", "driver", "points").toPandas())

fig, ax = plt.subplots(figsize=(9, 4.5))
for name, color in [("Michael Schumacher", AQUA), ("Lewis Hamilton", BLUE), ("Max Verstappen", YELLOW)]:
    d = trio[trio["driver"] == name].sort_values("season")
    ax.plot(d["season"], d["points"], color=color, linewidth=2, label=name)
    last = d.iloc[-1]
    ax.annotate(name.split()[-1], (last["season"], last["points"]),
                xytext=(6, 0), textcoords="offset points", fontsize=9, color=INK, va="center")
ax.set_title("Championship points per season — three eras of dominance", loc="left",
             fontsize=12, color=INK, pad=12)
ax.yaxis.grid(True, color=GRID, linewidth=0.8)
ax.set_axisbelow(True)
ax.legend(frameon=False, fontsize=9, labelcolor=INK)
ax.set_xlim(right=2031)  # room for the direct labels
clean_axes(ax)
plt.tight_layout()
plt.show()

# COMMAND ----------

pe_pd = spark.table("f1.gold.pit_stop_evolution").orderBy("season").toPandas()

fig, ax = plt.subplots(figsize=(9, 4))
ax.plot(pe_pd["season"], pe_pd["median_stop_s"], color=BLUE, linewidth=2)
best = pe_pd.loc[pe_pd["median_stop_s"].idxmin()]
ax.annotate(f"median {best['median_stop_s']:.1f}s ({int(best['season'])})",
            (best["season"], best["median_stop_s"]),
            xytext=(0, -16), textcoords="offset points", fontsize=9, color=INK, ha="center")
ax.set_title("Median pit-stop duration by season (seconds, 1994–2026)", loc="left",
             fontsize=12, color=INK, pad=12)
ax.yaxis.grid(True, color=GRID, linewidth=0.8)
ax.set_axisbelow(True)
clean_axes(ax)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## You just proved (with your own run)
# MAGIC
# MAGIC 1. **Every mart ties back to Silver** — 14 reconciliation checks against golden numbers computed
# MAGIC    from the source before this code ran. A dashboard on these marts cannot silently drift.
# MAGIC 2. **The layers work as designed:** Bronze preserved → Silver trusted → Gold answered. The first
# MAGIC    joins of the project were lossless *because* M2 verified every foreign key.
# MAGIC 3. **The data tells true stories:** Hamilton 106 · Schumacher 91 · Verstappen 71 — checkable
# MAGIC    against the real world, which is exactly what a reviewer will do.
# MAGIC
# MAGIC **Next:** record the verdict in the spec's Completion section, commit — then **M4**: the whole
# MAGIC Bronze → Silver → Gold flow as ONE Lakeflow Declarative Pipeline with native expectations.
