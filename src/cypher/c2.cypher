MATCH (n)
WHERE (n:Conference OR n:Journal)
MATCH (n)-[:HOLD_AT]-(:Edition)-[:PRESENTED_AT]-(a1:Article),
(n)-[:HOLD_AT]-(:Edition)-[:PRESENTED_AT]-(a2:Article)-[:IS_ABOUT]-(k:Keyword)-[:DEFINES]-(s:Community {community: 'database'})
WITH n, COUNT(DISTINCT a1) AS total_articles, COUNT(DISTINCT a2) as matching_articles 
WHERE toFloat(matching_articles)/total_articles >= 0.9
WITH n, toFloat(matching_articles)/total_articles AS matching_score
SET n.community = 'database'