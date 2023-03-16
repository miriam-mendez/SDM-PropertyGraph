
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = ""
DB = "neo4j"

from connection import Neo4jConnection

# %%    
conn = Neo4jConnection(URI, USER, PASSWORD, DB)

# %%
delete_all_edes = "MATCH (a) -[r] -> () DELETE a, r"
delete_all_nodes = "MATCH (a) DELETE a"
conn.query(delete_all_edes)
conn.query(delete_all_nodes)


# %%
authors = """
LOAD CSV WITH HEADERS FROM 'file:///authors.csv' AS row
MERGE (a:Author {id: toInteger(row.id), name: row.author})
"""
conn.query(authors)

# %%
conferences = """
LOAD CSV WITH HEADERS FROM 'file:///conferences.csv' AS row
MERGE (c:Conference {id: toInteger(row.id), name: row.conference})
"""

conn.query(conferences)

# %%
journals = """
LOAD CSV WITH HEADERS FROM 'file:///journals.csv' AS row
MERGE (j:Journal {id: toInteger(row.id), name: row.journal})
"""
conn.query(journals)

# %%
editions = """
LOAD CSV WITH HEADERS FROM 'file:///editions.csv' AS row
MERGE (e:Edition {id: toInteger(row.id), city: row.city, year:toInteger(row.year)})
WITH e, row
MATCH(c:Conference {id: toInteger(row.conference_id)})
MERGE (e)-[:HOLD_AT]-(c)
"""
conn.query(editions)

# %%
volumes = """
LOAD CSV WITH HEADERS FROM 'file:///volumes.csv' AS row
MERGE (v:Volume {id: toInteger(row.id), name: row.volume, year:toInteger(row.year)})
WITH v, row
MATCH (j:Journal {id: toInteger(row.journal_id)})
MERGE (v)-[:HOLD_AT]-(j)
"""
conn.query(volumes)

# %%
keywords = """
LOAD CSV WITH HEADERS FROM 'file:///keywords.csv' AS row
MERGE (k:Keyword {id: toInteger(row.id), name: row.keyword})
"""
conn.query(keywords)

# %%
query = """
LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS row
MERGE (p:Article {id: toInteger(row.id), title: row.title, doi: row.doi, pages:row.pages, abstract:row.abstract})
"""
conn.query(query)

query = """
LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
UNWIND split(row.keyword_ids, '|') as keyword
MATCH (k:Keyword {id: toInteger(keyword)})
MERGE (p)-[:IS_ABOUT]-(k)
"""
conn.query(query)

query = """
LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
UNWIND split(row.author_ids, '|') as author
MATCH (a:Author {id: toInteger(author)})
MERGE (a)-[:HAS_WRITTEN]-(p)
"""
conn.query(query)

query = """
LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
MATCH (e:Edition {id: toInteger(row.edition_id)})
MERGE (p)-[:PRESENTED_AT]-(e)
"""
conn.query(query)

query = """
LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
MATCH (v:Volume {id: toInteger(row.volume_id)})
MERGE (p)-[:PRESENTED_AT]-(v)
"""
conn.query(query)

# %%
corresponded_author = """
LOAD CSV WITH HEADERS FROM 'file:///corr_authors.csv' AS row
MATCH (p:Article {id: toInteger(row.id)}), (a:Author {id: toInteger(row.author_id)})
MERGE (p)-[:IS_CORR_OF]-(a)
"""
conn.query(corresponded_author)

# %%
citations = """
LOAD CSV WITH HEADERS FROM 'file:///citations.csv' AS row
MATCH (p1:Article {id: toInteger(row.citer_id)}), (p2:Article {id: toInteger(row.cited_id)})
MERGE (p1)-[r:CITES_TO]-(p2)
"""
conn.query(citations)

# %%
reviewers = """
LOAD CSV WITH HEADERS FROM 'file:///reviewers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
UNWIND split(row.author_ids, '|') as author
MATCH (a:Author {id: toInteger(author)})
MERGE (a)-[r:HAS_REVIEWED]-(p)
"""
conn.query(reviewers)

# %%
conn.close()