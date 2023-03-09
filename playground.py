#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')


# In[ ]:


from src.utils import RawData, generate_citations, check_citations_is_dag
from src.connection import Neo4jConnection

URI = "bolt://localhost:7687"
USER = "neo4j"
PWD = "enricmiriam"
DB = "sdm"


# In[ ]:


conn = Neo4jConnection(URI, USER, PWD, DB)


# In[ ]:


import os

file = os.path.abspath("data/authors.csv").replace('\\', '/')
file[3:]


# Load authors:

# In[ ]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/authors.csv' AS row
MERGE (a:Author {id: toInteger(row.key), name: row.name})
"""


# In[ ]:


conn.query(query)


# Load conferences:

# In[ ]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/conferences.csv' AS row
MERGE (c:Conference {id: toInteger(row.key), name: row.conference})
"""

conn.query(query)


# Load journals:

# In[ ]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/journals.csv' AS row
MERGE (j:Journal {id: toInteger(row.key), name: row.journal})
"""

conn.query(query)


# Load editions:

# In[ ]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/editions.csv' AS row
MERGE (e:Edition {id: toInteger(row.key), name: row.edition, year:toInteger(row.year)})
WITH e, row
MATCH(c:Conference {id: toInteger(row.conference_key)})
MERGE (e)-[r:HOLD_AT]-(c)
"""

conn.query(query)


# Load volumes:

# In[ ]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/volumes.csv' AS row
MERGE (v:Volume {id: toInteger(row.key), name: row.volume, year:toInteger(row.year)})
WITH v, row
MATCH(j:Journal {id: toInteger(row.conference_key)})
MERGE (v)-[r:HOLD_AT]-(j)
"""

conn.query(query)


# Load keywords:

# In[ ]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/keywords.csv' AS row
MERGE (k:Keyword {id: toInteger(row.key), name: row.keyword})
"""

conn.query(query)


# Load articles:

# In[ ]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/articles.csv' AS row
MERGE (p:Article {id: toInteger(row.key), title: row.title})
WITH p, row
UNWIND split(row.authors_key, '|') as author
MATCH (a:Author {id: toInteger(author)})
MERGE (a)-[r1:HAS_WRITTEN]-(p)
WITH p, row
MATCH (e:Edition {id: toInteger(row.edition_key)})
MERGE (p)-[r2:PRESENTED_AT]-(e)
WITH p, row
MATCH (v:Volume {id: toInteger(row.volume_key)})
MERGE (p)-[r3:PRESENTED_AT]-(v)
WITH p, row
UNWIND split(row.keywords_keys, '|') as keyword
MATCH (k:Keyword {id: toInteger(keyword)})
MERGE (p)-[r4:IS_ABOUT]-(k)
"""

conn.query(query)


# Load citations:

# In[ ]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/citations.csv' AS row
MATCH (p1:Article {id: toInteger(row.citer_key)})
MATCH (p2:Article {id: toInteger(row.citer_key)})
MERGE (p1)-[r:CITES_TO]-(p2)
"""

conn.query(query)


# In[ ]:


conn.close()

