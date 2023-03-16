CALL gds.louvain.stream('graph')
YIELD nodeId, communityId, intermediateCommunityIds
RETURN gds.util.asNode(nodeId).title AS Article, communityId
ORDER BY communityId ASC, Article ASC;