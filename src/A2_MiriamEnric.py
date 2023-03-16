URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = ""
DB = "neo4j"

from connection import Neo4jConnection

# %%    
conn = Neo4jConnection(URI, USER, PASSWORD, DB)

# %%
query = """
LOAD CSV WITH HEADERS FROM 'file:///reviews.csv' AS row
MATCH (:Author {id: toInteger(row.author_id)})-[r:HAS_REVIEWED]-(:Article {id: toInteger(row.article_id)})
SET r.review=row.content, r.decision=row.decision
"""
conn.query(query)

# %%
query = """
LOAD CSV WITH HEADERS FROM 'file:///organizations.csv' AS row
MERGE (o:Organization {id: toInteger(row.id), name: row.organization, type: row.type})
"""

conn.query(query)

# %%
query = """
LOAD CSV WITH HEADERS FROM 'file:///affiliations.csv' AS row
MATCH (a:Author {id: toInteger(row.id)})
SET a.affiliation=row.affiliation
"""
conn.query(query)

# %%
conn.close()
