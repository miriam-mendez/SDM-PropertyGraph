MATCH (a:Author)-[:HAS_WRITTEN]->(p:Article)-[:PRESENTED_AT]->(e:Edition)-[:HOLD_AT]->(c:Conference)
WITH c, a, COUNT(DISTINCT e) AS neditions
WHERE neditions >= 4
RETURN c, COLLECT(DISTINCT a.name) AS authors