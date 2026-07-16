-- Dataset: constructor_fortunes — "How have team fortunes shifted?"
-- The 5 all-time career-points constructors, season by season.
-- Widget: line — x=season, y=points, series=constructor.
-- Anchors: Ferrari 11,669.3 · Mercedes 8,440.6 · Red Bull 8,202 · McLaren 7,933.5 · Williams 3,776.
WITH career AS (
  SELECT constructor_id, SUM(points) AS career_points
  FROM f1.medallion.gold_constructor_season_summary
  GROUP BY constructor_id
  ORDER BY career_points DESC
  LIMIT 5
)
SELECT s.season, s.constructor, s.points, s.wins
FROM f1.medallion.gold_constructor_season_summary AS s
JOIN career USING (constructor_id)
ORDER BY s.season, s.constructor
