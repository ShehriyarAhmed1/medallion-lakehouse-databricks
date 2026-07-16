-- Dataset: circuit_history — "What's each track's history?"
-- Widget: table. Anchors: Monza / Silverstone / Monaco among the most-raced circuits.
SELECT circuit, country, races_held, first_season, last_season, distinct_winners
FROM f1.medallion.gold_circuit_stats
ORDER BY races_held DESC
LIMIT 15
