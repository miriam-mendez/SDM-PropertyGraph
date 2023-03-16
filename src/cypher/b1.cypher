MATCH (c:Conference)<-[:HOLD_AT]-(e:Edition)<-[:PRESENTED_AT]-(p:Article)<-[r:CITES_TO]-(q:Article)
WITH c, p, COUNT(r) AS num_citations
ORDER BY c.name, num_citations DESC
WITH c, COLLECT({title:p.title, citations:num_citations}) AS papers
RETURN c.name AS Conference, papers[..3] AS top_3_papers