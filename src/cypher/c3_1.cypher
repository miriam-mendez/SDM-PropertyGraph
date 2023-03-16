CALL gds.graph.project.cypher(
    'db-community',
    'MATCH (n) WHERE (n:Conference OR n:Journal) MATCH (n {community: "database"})-[:HOLD_AT]-()-[:PRESENTED_AT]-(p:Article) RETURN id(p) as id',
    'MATCH (p1)-[:CITES_TO]-(p2) RETURN id(p1) AS source, id(p2) AS target',
    {validateRelationships: False})
YIELD
    graphName,
    nodeQuery,
    nodeCount,
    relationshipQuery,
    relationshipCount,
    projectMillis