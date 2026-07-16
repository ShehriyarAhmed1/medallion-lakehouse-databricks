-- Dataset: pit_stop_evolution — "How have pit stops evolved?"
-- Widget: line — x=season, y=median_stop_s (+ avg_stop_s as a second series; same unit, one axis).
-- Anchors: ~30s medians in the refuelling era (1994–2009), sharp drop after the 2010 refuelling
-- ban, minimum median ≈22.3s in 2012. Durations are pit-LANE transit, not stationary time.
SELECT season, stops, avg_stop_s, median_stop_s, fastest_stop_s
FROM f1.medallion.gold_pit_stop_evolution
ORDER BY season
