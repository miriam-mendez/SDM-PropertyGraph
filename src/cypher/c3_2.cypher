CALL gds.pageRank.stream('db-community', {
  maxIterations: 20,
  dampingFactor: 0.85
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId).title AS name, score
ORDER BY score DESC, name ASC
LIMIT 100
MATCH (p:Article {title: name}), (s:Community {community: 'database'})
MERGE (p)-[:TOP_OF]-(s)