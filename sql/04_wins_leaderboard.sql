-- Dataset: wins_leaderboard — "Who wins the most, ever?"
-- Widget: horizontal bar — y=driver, x=wins (podiums/points as tooltip columns).
-- Anchors: Hamilton 106 · Schumacher 91 · Verstappen 71 (matches real F1 history).
SELECT
  driver,
  SUM(wins)             AS wins,
  SUM(podiums)          AS podiums,
  ROUND(SUM(points), 1) AS career_points
FROM f1.medallion.gold_driver_season_summary
GROUP BY driver
ORDER BY wins DESC
LIMIT 10
