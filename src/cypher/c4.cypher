MATCH (a:Author)-[:HAS_WRITTEN]-(p:Article)-[:TOP_OF]-(s:Community {community: 'database'})
WITH a, count(p) AS articleCount
WHERE articleCount >= 2
RETURN a.name AS authorName, collect(DISTINCT articleCount) AS articleCounts