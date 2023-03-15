#!/usr/bin/env python
# coding: utf-8

# In[15]:


get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')


# In[280]:


from src.utils import RawData, generate_citations, check_citations_is_dag
from src.connection import Neo4jConnection

URI = "bolt://localhost:7687"
USER = "neo4j"
PWD = "enricmiriam"
DB = "sdm"


# CHANGE p:Article to p:Paper

# In[281]:


conn = Neo4jConnection(URI, USER, PWD, DB)


# In[282]:


delete_all_edes = "MATCH (a) -[r] -> () DELETE a, r"
delete_all_nodes = "MATCH (a) DELETE a"


# In[283]:


conn.query(delete_all_edes)
conn.query(delete_all_nodes)


# Load authors:

# In[284]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/authors.csv' AS row
MERGE (a:Author {id: toInteger(row.id), name: row.author})
"""

conn.query(query)


# Load conferences:

# In[285]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/conferences.csv' AS row
MERGE (c:Conference {id: toInteger(row.id), name: row.conference})
"""

conn.query(query)


# Load journals:

# In[286]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/journals.csv' AS row
MERGE (j:Journal {id: toInteger(row.id), name: row.journal})
"""

conn.query(query)


# Load editions:

# In[287]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/editions.csv' AS row
MERGE (e:Edition {id: toInteger(row.id), city: row.city, year:toInteger(row.year)})
WITH e, row
MATCH(c:Conference {id: toInteger(row.conference_id)})
MERGE (e)-[:HOLD_AT]-(c)
"""

conn.query(query)


# Load volumes:

# In[288]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/volumes.csv' AS row
MERGE (v:Volume {id: toInteger(row.id), name: row.volume, year:toInteger(row.year)})
WITH v, row
MATCH (j:Journal {id: toInteger(row.journal_id)})
MERGE (v)-[:HOLD_AT]-(j)
"""

conn.query(query)


# Load keywords:

# In[289]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/keywords.csv' AS row
MERGE (k:Keyword {id: toInteger(row.id), name: row.keyword})
"""

conn.query(query)


# Load papers:

# In[290]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/papers.csv' AS row
MERGE (p:Article {id: toInteger(row.id), title: row.title, doi: row.doi, pages:row.pages, abstract:row.abstract})
"""
conn.query(query)

query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/papers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
UNWIND split(row.keyword_ids, '|') as keyword
MATCH (k:Keyword {id: toInteger(keyword)})
MERGE (p)-[:IS_ABOUT]-(k)
"""
conn.query(query)

query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/papers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
UNWIND split(row.author_ids, '|') as author
MATCH (a:Author {id: toInteger(author)})
MERGE (a)-[:HAS_WRITTEN]-(p)
"""
conn.query(query)

query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/papers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
MATCH (e:Edition {id: toInteger(row.edition_id)})
MERGE (p)-[:PRESENTED_AT]-(e)
"""
conn.query(query)

query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/papers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
MATCH (v:Volume {id: toInteger(row.volume_id)})
MERGE (p)-[:PRESENTED_AT]-(v)
"""
conn.query(query)


# Load correspondant authors:

# In[291]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/corr_authors.csv' AS row
MATCH (p:Article {id: toInteger(row.id)}), (a:Author {id: toInteger(row.author_id)})
MERGE (p)-[:IS_CORR_OF]-(a)
"""

conn.query(query)


# Load citations:

# In[292]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/citations.csv' AS row
MATCH (p1:Article {id: toInteger(row.citer_id)}), (p2:Article {id: toInteger(row.cited_id)})
MERGE (p1)-[r:CITES_TO]-(p2)
"""

conn.query(query)


# Load reviewers:

# In[293]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/reviewers.csv' AS row
MATCH (p:Article {id: toInteger(row.id)})
UNWIND split(row.author_ids, '|') as author
MATCH (a:Author {id: toInteger(author)})
MERGE (a)-[r:HAS_REVIEWED]-(p)
"""

conn.query(query)


# Expand graph:

# In[294]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/reviews.csv' AS row
MATCH (:Author {id: toInteger(row.author_id)})-[r:HAS_REVIEWED]-(:Article {id: toInteger(row.article_id)})
SET r.review=row.content, r.decision=row.decision
"""

conn.query(query)


# In[295]:


query = """
LOAD CSV WITH HEADERS FROM 'file:///Users/ereve/projects/mds/sdm/data/affiliations.csv' AS row
MATCH (a:Author {id: toInteger(row.id)})
SET a.affiliation=row.affiliation
"""

conn.query(query)


# Recommender system:
# 

# In[231]:


import pandas as pd

keywords = pd.read_csv('data/keywords.csv')
keywords['keyword'] = keywords['keyword']
data_keywords = keywords[keywords['keyword'].str.contains('Data|data')]['keyword'].to_list()
data_keywords


# In[110]:


data_keywords = ['data mining', 'big data', 'data warehousing', 'data preprocessing', 'data integration', 'data cleaning', 'data governance']


# In[ ]:


query_conferences = """
MATCH (c:Conference)-[:HOLD_AT]-(:Edition)-[:PRESENTED_AT]-(a:Article)-[:IS_ABOUT]-(k:Keyword)
WITH c, COUNT(DISTINCT a) AS total_articles, 
     COUNT(DISTINCT CASE WHEN ANY(keyword IN ['Data', 'data'] WHERE k.name CONTAINS keyword) THEN a END) AS matching_articles
WHERE toFloat(matching_articles)/total_articles >= 0.8
RETURN c, toFloat(matching_articles)/total_articles
"""

query_journals = """
MATCH (j:Journal)-[:HOLD_AT]-(:Volume)-[:PRESENTED_AT]-(a:Article)-[:IS_ABOUT]-(k:Keyword)
WITH j, COUNT(DISTINCT a) AS total_articles, 
     COUNT(DISTINCT CASE WHEN ANY(keyword IN ['Data', 'data'] WHERE k.name CONTAINS keyword) THEN a END) AS matching_articles
WHERE toFloat(matching_articles)/total_articles >= 0.9
RETURN j, toFloat(matching_articles)/total_articles
"""


# In[ ]:


# Change id:0 for the one of the found communities

# CALL gds.graph.drop('db-community')

query_project_community = """
CALL gds.graph.project.cypher(
    'db-community',
    'MATCH (:Conference {id: 0})-[:HOLD_AT]-(:Edition)-[:PRESENTED_AT]-(p:Article) RETURN id(p) as id',
    'MATCH (p1)-[:CITES_TO]-(p2) RETURN id(p1) AS source, id(p2) AS target',
    {validateRelationships: False})
YIELD
    graphName,
    nodeQuery,
    nodeCount,
    relationshipQuery,
    relationshipCount,
    projectMillis
"""

query_page_rank_get_gurus ="""
CALL gds.pageRank.stream('db-community', {
  maxIterations: 20,
  dampingFactor: 0.85
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId).title AS name, score
ORDER BY score DESC, name ASC
LIMIT 10
MATCH (a:Author)-[:HAS_WRITTEN]->(p:Article)
WHERE p.title IN [name]
WITH a, count(p) AS articleCount
WHERE articleCount >= 2
RETURN a.name AS authorName, collect(DISTINCT articleCount) AS articleCounts
"""


# Third query: impact factors

# In[ ]:


query = """"
MATCH (j:Journal)-[:HOLD_AT]-(v:Volume)-[:PRESENTED_AT]-(p:Article)-[c:CITES_TO]-(:Article)
WITH j.name AS journal_name, v.year AS year, toFloat(COUNT(c)) AS ncitations
CALL {
    WITH journal_name, year
    MATCH (:Journal {name: journal_name})-[:HOLD_AT]-(v:Volume)-[r:PRESENTED_AT]-(:Article)
    WHERE v.year IN [year-1, year-2]
    RETURN toFloat(COUNT(r)) AS preceding_count
}
RETURN journal_name, year, CASE preceding_count WHEN 0 THEN -1 ELSE ncitations/preceding_count END AS if
ORDER BY if DESC
"""


# Inject more citations

# In[230]:


# from src.utils import generate_citations
# import csv

# papers = pd.read_csv('data/papers.csv')
# editions = pd.read_csv('data/editions.csv')
# volumes = pd.read_csv('data/volumes.csv')

# papers = papers.merge(editions, how='left', left_on='edition_id', right_on='id')
# papers.rename({'id_x': 'id'}, axis=1, inplace=True)
# papers = papers.merge(volumes, how='left', left_on='volume_id', right_on='id')
# papers.rename({'id_x': 'id'}, axis=1, inplace=True)
# papers = papers[['id', 'year_x', 'year_y']]
# papers['year'] = papers['year_x'].fillna(papers['year_y'])
# papers.drop(['year_x', 'year_y'], axis=1, inplace=True)


# citations = generate_citations(papers, alpha=5)
# citations.to_csv('data/citations.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)


# Inject more "data" keywords to specific conference

# In[273]:


import numpy as np

papers = pd.read_csv('data/papers.csv')
keywords = pd.read_csv('data/keywords.csv')
editions = pd.read_csv('data/editions.csv')

keywords['keyword'] = keywords['keyword']
data_keywords = keywords[keywords['keyword'].str.contains('Data|data')]['keyword'].to_list()

def inject_keywords_to_conf(papers, editions, keywords, keyword_list, conference_id):
    # Filter papers of conference
    papers = papers.merge(editions, how='left', left_on='edition_id', right_on='id')
    papers.rename({'id_x': 'id'}, inplace=True, axis=1)

    # Get keyword ids (of interest)
    keyword_dict = {v: k for k, v in keywords['keyword'].to_dict().items()}
    keyword_ids = [keyword_dict[keyword] for keyword in keyword_list]

    # Inject with random probability 
    indices_list = []
    keyword_ids_list =[]
    for ix, row in papers.iterrows():
        indices_list.append(ix)
        if np.random.random() < 0.95 and row.conference_id == conference_id:
            new_keyword_ids = row.keyword_ids + f'|{np.random.choice(keyword_ids)}'
        else:
            new_keyword_ids = row.keyword_ids
        keyword_ids_list.append(new_keyword_ids)

    return pd.DataFrame({
        'id': indices_list,
        'keyword_ids': keyword_ids_list
    })


# In[274]:


df = inject_keywords_to_conf(papers, editions, keywords, data_keywords, 0)


# In[275]:


papers.head()


# In[276]:


df['len'] = df['keyword_ids'].apply(lambda x: len(x.split('|')))
df.groupby('len').count()


# In[278]:


papers['keyword_ids'] = df['keyword_ids']


# In[279]:


import csv
papers.to_csv('data/papers.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

