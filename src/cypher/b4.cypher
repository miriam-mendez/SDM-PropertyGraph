MATCH (a:Author)-[:HAS_WRITTEN]->(p:Article)<-[:CITES_TO]-(:Article)
WITH a.name AS Author, p.title AS Title, count(*) AS ncites
ORDER BY ncites DESC
WITH Author, collect(ncites) as Cites
WITH Author, [x IN range(1, size(Cites)) WHERE x <= Cites[x-1] |  [Cites[x-1],x]] 
AS Hindexes
RETURN Author, Hindexes[-1][1] as Hindex
ORDER BY Hindex desc