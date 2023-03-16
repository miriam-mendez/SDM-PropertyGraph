CREATE (s:Community {id: 0, community: 'database'})
MATCH (k:Keyword)
WHERE ANY(keyword IN ['Data', 'data'] WHERE k.name CONTAINS keyword)
MERGE (s)-[:DEFINES]-(k)