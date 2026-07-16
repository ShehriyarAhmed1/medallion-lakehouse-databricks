-- Dataset: kpi_headline — the dashboard's top counter strip (one row, five KPIs).
-- Widget: 5 counters. Expected: 77 · 1,171 · 865 · 1,161 · 22,475 (verified from source).
SELECT
  (SELECT COUNT(DISTINCT season)    FROM f1.medallion.gold_driver_season_summary) AS seasons_covered,
  (SELECT SUM(races_held)           FROM f1.medallion.gold_circuit_stats)         AS grands_prix,
  (SELECT COUNT(DISTINCT driver_id) FROM f1.medallion.gold_driver_season_summary) AS drivers_all_time,
  (SELECT SUM(wins)                 FROM f1.medallion.gold_driver_season_summary) AS race_wins_recorded,
  (SELECT SUM(stops)                FROM f1.medallion.gold_pit_stop_evolution)    AS pit_stops_recorded
