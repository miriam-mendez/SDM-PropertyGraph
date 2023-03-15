#!/usr/bin/env python
# coding: utf-8

# In[15]:


get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')


# In[16]:


from src.utils import RawData, generate_citations, check_citations_is_dag
from src.connection import Neo4jConnection

URI = "bolt://localhost:7687"
USER = "neo4j"
PWD = "enricmiriam"
DB = "sdm"


# CHANGE p:Article to p:Paper

# In[59]:


conn = Neo4jConnection(URI, USER, PWD, DB)


# In[18]:


delete_all_edes = "MATCH (a) -[r] -> () DELETE a, r"
delete_all_nodes = "MATCH (a) DELETE a"


# In[19]:


conn.query(delete_all_edes)
conn.query(delete_all_nodes)


# Load authors:

# In[20]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/authors.csv' AS row
MERGE (a:Author {id: toInteger(row.id), name: row.author})
"""

conn.query(query)


# Load conferences:

# In[22]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/conferences.csv' AS row
MERGE (c:Conference {id: toInteger(row.id), name: row.conference})
"""

conn.query(query)


# Load journals:

# In[23]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/journals.csv' AS row
MERGE (j:Journal {id: toInteger(row.id), name: row.journal})
"""

conn.query(query)


# Load editions:

# In[24]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/editions.csv' AS row
MERGE (e:Edition {id: toInteger(row.id), city: row.city, year:toInteger(row.year)})
WITH e, row
MATCH(c:Conference {id: toInteger(row.conference_id)})
MERGE (e)-[r:HOLD_AT]-(c)
"""

conn.query(query)


# Load volumes:

# In[25]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/volumes.csv' AS row
MERGE (v:Volume {id: toInteger(row.id), name: row.volume, year:toInteger(row.year)})
WITH v, row
MATCH(j:Journal {id: toInteger(row.journal_id)})
MERGE (v)-[r:HOLD_AT]-(j)
"""

conn.query(query)


# Load keywords:

# In[26]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/keywords.csv' AS row
MERGE (k:Keyword {id: toInteger(row.id), name: row.keyword})
"""

conn.query(query)


# Load papers:

# In[48]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/papers.csv' AS row
MERGE (p:Article {id: toInteger(row.id), title: row.title, doi: row.doi, pages:row.pages, abstract:row.abstract})
WITH p, row
UNWIND split(row.author_ids, '|') as author
MATCH (a:Author {id: toInteger(author)})
MERGE (a)-[r1:HAS_WRITTEN]-(p)
WITH p, row
MATCH (e:Edition {id: toInteger(row.edition_id)})
MERGE (p)-[r2:PRESENTED_AT]-(e)
WITH p, row
MATCH (v:Volume {id: toInteger(row.volume_id)})
MERGE (p)-[r3:PRESENTED_AT]-(v)
WITH p, row
UNWIND split(row.keyword_ids, '|') as keyword
MATCH (k:Keyword {id: toInteger(keyword)})
MERGE (p)-[r4:IS_ABOUT]-(k)
"""

conn.query(query)


# Load correspondant authors:

# In[51]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/corr_authors.csv' AS row
MATCH (p:Article {id: toInteger(row.id)}), (a:Author {id: toInteger(row.auhtor_id)}) 
MERGE (a)-[r:IS_CORR_OF]-(p)
"""

conn.query(query)


# Load citations:

# In[56]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/citations.csv' AS row
MATCH (p1:Article {id: toInteger(row.citer_id)}), (p2:Article {id: toInteger(row.cited_id)})
MERGE (p1)-[r:CITES_TO]-(p2)
"""

conn.query(query)


# Load reviewers:

# In[53]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/reviewers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
WITH p, row
UNWIND split(row.author_ids, '|') as author
MATCH (a:Author {id: toInteger(author)})
MERGE (a)-[r:HAS_REVIEWED]-(p)
"""

conn.query(query)


# Expand graph:

# In[64]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/reviews.csv' AS row
MATCH (:Author {id: toInteger(row.author_id)})-[r:HAS_REVIEWED]-(:Article {id: toInteger(row.article_id)})
WITH r, row
SET r.review=row.content, r.decision=row.decision
"""

conn.query(query)


# In[65]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/affiliations.csv' AS row
MATCH (a:Author {id: toInteger(row.id)})
WITH a, row
SET a.affiliation=row.affiliation
"""

conn.query(query)


# Testing queries:
# 

# In[62]:


# query = """
# MATCH (p1:Article)<-[:CITES_TO]-(p2:Article)-[:PRESENTED_AT]->(e:Edition)-[:HOLD_AT]->(c:Conference)
# WITH p1,p2,count(p1) as ncites
# ORDER BY ncites DESC
# RETURN distinct p2.title as Title, ncites
# LIMIT 3
# """

# result = conn.query(query, get_results=True)

