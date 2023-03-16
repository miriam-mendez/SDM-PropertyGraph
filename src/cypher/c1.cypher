CREATE (s:Community {id: 0, community: 'database'})
WITH s
MATCH (k:Keyword)
WHERE ANY(keyword IN ['Data', 'data'] WHERE k.name CONTAINS keyword)
MERGE (s)-[:DEFINES]-(k)