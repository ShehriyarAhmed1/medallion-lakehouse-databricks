-- Dataset: driver_eras — "Who dominated each era?"
-- The 5 all-time career-points leaders, season by season.
-- Widget: line — x=season, y=points, series=driver.
-- Anchors: career totals Hamilton 5,082.5 · Verstappen 3,368.5 · Vettel 3,098 · Alonso 2,381 · Räikkönen 1,873.
-- Note: points are AS RECORDED per era (10 for a 2008 win, 25 today) — never re-scored.
WITH career AS (
  SELECT driver_id, SUM(points) AS career_points
  FROM f1.medallion.gold_driver_season_summary
  GROUP BY driver_id
  ORDER BY career_points DESC
  LIMIT 5
)
SELECT s.season, s.driver, s.points, s.wins
FROM f1.medallion.gold_driver_season_summary AS s
JOIN career USING (driver_id)
ORDER BY s.season, s.driver
