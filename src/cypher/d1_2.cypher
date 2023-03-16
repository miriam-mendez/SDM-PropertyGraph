CALL gds.nodeSimilarity.stream('bigraph')
YIELD node1, node2, similarity
WHERE gds.util.asNode(node1).title < gds.util.asNode(node2).title
RETURN distinct gds.util.asNode(node1).title AS Article1, gds.util.asNode(node2).title AS Article2, similarity
ORDER BY similarity DESCENDING, Article1, Article2