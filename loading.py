#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')


# In[2]:


from src.utils import RawData, generate_citations, check_citations_is_dag
from src.connection import Neo4jConnection

URI = "bolt://localhost:7687"
USER = "neo4j"
PWD = ""
DB = ""


# In[85]:


conn = Neo4jConnection(URI, USER, PWD, DB)


# In[107]:


delete_all_edes = "MATCH (a) -[r] -> () DELETE a, r"
delete_all_nodes = "MATCH (a) DELETE a"


# In[108]:


conn.query(delete_all_edes)
conn.query(delete_all_nodes)


# Load authors:

# In[109]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/authors.csv' AS row
MERGE (a:Author {id: toInteger(row.id), name: row.author})
"""

conn.query(query)


# Load conferences:

# In[110]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/conferences.csv' AS row
MERGE (c:Conference {id: toInteger(row.id), name: row.conference})
"""

conn.query(query)


# Load journals:

# In[111]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/journals.csv' AS row
MERGE (j:Journal {id: toInteger(row.id), name: row.journal})
"""

conn.query(query)


# Load editions:

# In[112]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/editions.csv' AS row
MERGE (e:Edition {id: toInteger(row.id), city: row.city, year:toInteger(row.year)})
WITH e, row
MATCH(c:Conference {id: toInteger(row.conference_id)})
MERGE (e)-[:HOLD_AT]-(c)
"""

conn.query(query)


# Load volumes:

# In[113]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/volumes.csv' AS row
MERGE (v:Volume {id: toInteger(row.id), name: row.volume, year:toInteger(row.year)})
WITH v, row
MATCH (j:Journal {id: toInteger(row.journal_id)})
MERGE (v)-[:HOLD_AT]-(j)
"""

conn.query(query)


# Load keywords:

# In[114]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/keywords.csv' AS row
MERGE (k:Keyword {id: toInteger(row.id), name: row.keyword})
"""

conn.query(query)


# Load papers:

# In[115]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/papers.csv' AS row
MERGE (p:Article {id: toInteger(row.id), title: row.title, doi: row.doi, pages:row.pages, abstract:row.abstract})

WITH p, row
UNWIND split(row.keyword_ids, '|') as keyword
MATCH (k:Keyword {id: toInteger(keyword)})
MERGE (p)-[:IS_ABOUT]-(k)

WITH p, row
UNWIND split(row.author_ids, '|') as author
MATCH (a:Author {id: toInteger(author)})
MERGE (a)-[:HAS_WRITTEN]-(p)

WITH p, row
MATCH (e:Edition {id: toInteger(row.edition_id)})
MERGE (p)-[:PRESENTED_AT]-(e)

WITH p, row
MATCH (v:Volume {id: toInteger(row.volume_id)})
MERGE (p)-[:PRESENTED_AT]-(v)
"""

conn.query(query)


# In[116]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/papers.csv' AS row
MERGE (p:Article {id: toInteger(row.id), title: row.title, doi: row.doi, pages:row.pages, abstract:row.abstract})

WITH p, row
MATCH (v:Volume {id: toInteger(row.volume_id)})
MERGE (p)-[:PRESENTED_AT]-(v)
"""

conn.query(query)


# Load corresponding authors:

# In[117]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/corr_authors.csv' AS row
MATCH (p:Article {id: toInteger(row.id)}), (a:Author {id: toInteger(row.author_id)})
MERGE (p)-[:IS_CORR_OF]-(a)
"""

conn.query(query)


# Load citations:

# In[118]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/citations.csv' AS row
MATCH (p1:Article {id: toInteger(row.citer_id)}), (p2:Article {id: toInteger(row.cited_id)})
MERGE (p1)-[r:CITES_TO]-(p2)
"""

conn.query(query)


# Load reviewers:

# In[119]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/reviewers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
UNWIND split(row.author_ids, '|') as author
MATCH (a:Author {id: toInteger(author)})
MERGE (a)-[r:HAS_REVIEWED]-(p)
"""

conn.query(query)


# Expand graph:

# Load reviewers:

# In[120]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/reviews.csv' AS row
MATCH (:Author {id: toInteger(row.author_id)})-[r:HAS_REVIEWED]-(:Article {id: toInteger(row.article_id)})
SET r.review=row.content, r.decision=row.decision
"""

conn.query(query)


# Load organizations:

# In[121]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/organizations.csv' AS row
MERGE (o:Organization {id: toInteger(row.id), name: row.organization, type: row.type})
"""

conn.query(query)


# Load affiliations:

# In[122]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/affiliations.csv' AS row
MATCH (a:Author {id: toInteger(row.id)}), (o:Organization {id: toInteger(row.organization_id)})
MERGE (a)-[:AFFILIATED_WITH]-(o)
"""

conn.query(query)


# In[3]:


with open('src/cypher/c1.cypher', 'r') as f:
    query = f.read()


# In[11]:


import src.run_c

src.run_c

