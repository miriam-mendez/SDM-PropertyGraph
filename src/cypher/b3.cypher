MATCH (j:Journal)<-[:HOLD_AT]-(v:Volume)<-
[:PRESENTED_AT]-(p:Article)<-[c:CITES_TO]-(:Article)
WITH j.name AS journal_name, v.year AS year, toFloat(COUNT(c)) AS ncitations
CALL {
    WITH journal_name, year
    MATCH (:Journal {name: journal_name})-[:HOLD_AT]-(v:Volume)-
    [r:PRESENTED_AT]-(:Article)
    WHERE v.year IN [year-1, year-2]
    RETURN toFloat(COUNT(r)) AS preceding_count
}
RETURN journal_name, year, CASE preceding_count WHEN 0 THEN -1 
ELSE ncitations/preceding_count END AS if
ORDER BY if DESC