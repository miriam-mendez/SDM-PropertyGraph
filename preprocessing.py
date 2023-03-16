#!/usr/bin/env python
# coding: utf-8

# ### Download the data

# In[ ]:


# !wget https://dblp.dagstuhl.de/xml/dblp.xml.gz
# !wget https://dblp.dagstuhl.de/xml/dblp.dtd
# !gunzip dblp.xml.gz


# Clone the repo

# In[ ]:


# !git clone https://github.com/ThomHurks/dblp-to-csv.git


# Get requirements

# In[ ]:


# !pip install lxml
# !pip install -r dblp-to-csv/requirements.txt


# Allow changes to the files, parse into csv (takes some minutes)

# In[ ]:


# !chmod +x XMLToCSV.py
# !./XMLToCSV.py --annotate --neo4j ./dblp.xml ./dblp.dtd output.csv --relations author:authored_by journal:published_in publisher:published_by school:submitted_at editor:edited_by cite:has_citation series:is_part_of


# ### Pre-process the data

# In[ ]:


import pandas as pd
pd.options.display.max_columns = None

import numpy as np
import csv

class CFG:
    def __init__(self, seed=3):
        self.seed = 3
        self.rng = np.random.default_rng(self.seed)

class DBLP:
    def __init__(self):
        self.article_header = pd.read_csv('output_article_header.csv', sep=';')
        self.inproceedings_header = pd.read_csv('output_inproceedings_header.csv', sep=';')

        # Main
        self.articles = pd.read_csv('output_article.csv', header=None, names=self.article_header.columns, sep=';', nrows=1e6)
        self.inproceedings = pd.read_csv('output_inproceedings.csv', header=None, names=self.inproceedings_header.columns, sep=';', nrows=1e6)

        # RDBS
        self.author = pd.read_csv('output_author.csv', sep=';')
        self.author_authored_by = pd.read_csv('output_author_authored_by.csv', sep=';')
        self.cite = pd.read_csv('output_cite.csv', sep=';')
        self.cite_has_citation = pd.read_csv('output_cite_has_citation.csv', sep=';')
        self.editor = pd.read_csv('output_editor.csv', sep=';')
        self.editor_edited_by = pd.read_csv('output_editor_edited_by.csv', sep=';')
        self.journal = pd.read_csv('output_journal.csv', sep=';')
        self.journal_published_in = pd.read_csv('output_journal_published_in.csv', sep=';')
        self.publisher = pd.read_csv('output_publisher.csv', sep=';')
        self.publisher_published_by = pd.read_csv('output_publisher_published_by.csv', sep=';')
        self.school = pd.read_csv('output_school.csv', sep=';')
        self.school_submitted_at = pd.read_csv('output_school_submitted_at.csv', sep=';')
        self.series = pd.read_csv('output_series.csv', sep=';')
        self.series_is_part_of = pd.read_csv('output_series_is_part_of.csv', sep=';')


# In[ ]:


dblp = DBLP()
cfg = CFG()


# Get subset of the data:

# In[ ]:


def get_subset(dblp, cfg, n=100, conference_subset=['IGARSS', 'EUSIPCO', 'WCNC', 'NeurIPS'], journal_subset=['CoRR', 'Remote. Sens.', 'Entropy', 'IEEE Trans. Biomed. Eng.']):
    articles = dblp.articles[dblp.articles['journal:string'].isin(journal_subset)].sample(n, random_state=cfg.seed)
    inproceedings = dblp.inproceedings[dblp.inproceedings['booktitle:string'].isin(conference_subset)].sample(n, random_state=cfg.seed)
    return articles, inproceedings

articles, inproceedings = get_subset(dblp, cfg, n=500)


# Get columns of interest

# In[ ]:


articles_columns = ['article:ID', 'author:string[]', 'journal:string', 'pages:string', 'title:string', 'volume:string', 'year:int']
articles_sub = articles[articles_columns]
articles_sub.columns = ['id', 'author:string[]', 'journal:string', 'pages:string', 'title:string', 'volume:string', 'year']
articles_sub.head()


# Auxiliary function to replace volume random values

# In[ ]:


def replace_incorrect_volumes(volume):
    if pd.isna(volume):
        return volume
    elif isinstance(volume, int):
        return volume
    else:
        return volume[-1]


# In[ ]:


articles_sub['volume:string'] = articles_sub['volume:string'].apply(replace_incorrect_volumes)


# Do the same for inproceedings

# In[ ]:


inproceedings_columns = ['inproceedings:ID', 'author:string[]', 'booktitle:string', 'pages:string', 'title:string', 'year:int']
inproceedings_sub = inproceedings[inproceedings_columns]
inproceedings_sub.columns = ['id', 'author:string[]', 'booktitle:string', 'pages:string', 'title:string', 'year']
inproceedings_sub.head()


# Join both types of papers

# In[ ]:


papers = pd.concat([articles_sub, inproceedings_sub], axis=0)
# papers.replace('NaN', pd.NA)
papers.dropna(subset=['author:string[]'], axis=0, inplace=True)
papers.head()


# In[ ]:


# Reset index
papers = papers.reset_index(drop=True).reset_index().drop('id', axis=1).rename(columns={'index': 'id'})


# In[ ]:


def fill_missing_pages(pages):
    if pd.isna(pages):
        i = np.random.randint(100)
        return f'{i}-{i + np.random.randint(5, 20)}'
    else:
        return pages

papers['pages:string'] = papers['pages:string'].apply(lambda x: fill_missing_pages(x))


# Get authors

# In[ ]:


import re
author_list = [author for authors in papers['author:string[]'].tolist() for author in authors.split('|')]
author_set = set(author_list)
author_sub = dblp.author[dblp.author['author:string'].isin(author_list)]
authors = author_sub.reset_index(drop=True).reset_index().drop(':ID', axis=1).rename(columns={'index': 'id'})

def increase_author_rates(authors, author_list=list(author_set), n_gurus=50):
    if np.random.random() > 0.5:
        return authors + '|' + author_list[np.random.randint(len(author_list[:n_gurus]))]
    return authors

papers['author:string[]'] = papers['author:string[]'].apply(lambda x: increase_author_rates(x))

def get_correspondent_author(authors):
    return np.random.choice(authors.split('|'))


# Get editions, conferences, volumes, and journals

# In[ ]:


editions = papers[['booktitle:string', 'year']].value_counts()
editions = editions.reset_index(name='count').drop('count', axis=1)
editions['city'] = cfg.rng.choice(['Barcelona', 'Madrid', 'Paris', 'London', 'Denmark', 'Italy'], size=len(editions))
editions = editions.reset_index().rename(columns={'index': 'id'})

conferences = pd.DataFrame(editions['booktitle:string'].unique(), columns=['conference'])
conferences = conferences.reset_index().rename(columns={'index': 'id'})

volumes = papers[['journal:string', 'volume:string', 'year']].value_counts()
volumes = volumes.reset_index(name='count').drop('count', axis=1)
volumes = volumes.reset_index().rename(columns={'index': 'id'})

journals = pd.DataFrame(volumes['journal:string'].unique(), columns=['journal'])
journals = journals.reset_index().rename(columns={'index': 'id'})


# Auxiliary function to replace strings for its keys

# In[ ]:


def replace_values_for_ids(fact, dimension, fact_id, dimension_id, delim='|'):
    dfact = fact.copy()
    ddims = dimension.copy()

    if isinstance(fact_id, list):
        fact_id_new = '_'.join(fact_id)
        dfact[fact_id_new] = dfact[fact_id[0]].map(str)
        for fact_id_i in fact_id[1:]:
            dfact[fact_id_new] += dfact[fact_id_i].map(str)
        fact_id = fact_id_new

    if isinstance(dimension_id, list):
        dimension_id_new = '_'.join(dimension_id)
        ddims[dimension_id_new] = ddims[dimension_id[0]].map(str)
        for dimension_id_i in dimension_id[1:]:
            ddims[dimension_id_new] += ddims[dimension_id_i].map(str)
        dimension_id = dimension_id_new

    aux_dict = ddims.set_index(dimension_id)['id'].to_dict()
    
    def replace_value_for_id(x):
        try:
            return delim.join([str(aux_dict[id]) for id in x.split(delim)])
        except:
            return pd.NA
                          
    return dfact[fact_id].apply(lambda x: replace_value_for_id(x))


# In[ ]:


papers['author_ids'] = replace_values_for_ids(papers, authors, 'author:string[]', 'author:string')
papers['volume_id'] = replace_values_for_ids(papers, volumes, ['journal:string', 'volume:string', 'year'], ['journal:string', 'volume:string', 'year'])
papers['edition_id'] = replace_values_for_ids(papers, editions, ['booktitle:string', 'year'], ['booktitle:string', 'year'])
papers.drop(['author:string[]', 'journal:string', 'volume:string', 'booktitle:string'], axis=1, inplace=True)
papers.head()


# In[ ]:


editions['conference_id'] = replace_values_for_ids(editions, conferences, 'booktitle:string', 'conference')
editions.drop(['booktitle:string'], axis=1, inplace=True)
volumes['journal_id'] = replace_values_for_ids(volumes, journals, 'journal:string', 'journal')
volumes.drop(['journal:string'], axis=1, inplace=True)


# Build citations

# In[ ]:


import numpy as np
import math
ALPHA = 2 # The higher the most citations we will get

def get_citations(df, alpha=ALPHA): # It is a DAG, do not worry, check with nx
    max_year = df['year'].max()
    citing_articles = []
    cited_articles = []
    for ix, article in df.iterrows():
        n_possible_cited = np.random.randint(0, math.ceil((max_year-article.year)*alpha+0.01))
        
        for ix, cited in df.sample(n=n_possible_cited).iterrows():
            if article.year > cited.year:
                citing_articles.append(article.id)
                cited_articles.append(cited.id)

    citations = pd.DataFrame({'citer_id': citing_articles, 
                            'cited_id': cited_articles})
    
    return citations

citations = get_citations(papers)
papers.drop(['year'], axis=1, inplace=True)


# Generate abstracts and other properties

# In[ ]:


get_ipython().system('pip install transformers')


# In[ ]:


from transformers import pipeline, set_seed
generator = pipeline('text-generation', model='gpt2')

def generate_abstract(generator, text='In this paper,', max_length=100, num_return_sequences=1, seed=3):
    return generator(text, max_length=max_length, num_return_sequences=num_return_sequences)[0]['generated_text']

abstracts = [generate_abstract(generator) for _ in range(20)]

def get_choice_from_abstracts(abstracts):
    return abstracts[np.random.randint(len(abstracts))]

papers['abstract'] = [get_choice_from_abstracts(abstracts) for _ in range(len(papers))]


# Note: keywords chosen by GPT

# In[ ]:


keywords = [    "Machine learning",     "Artificial intelligence",     "Data mining",     "Big data",     "Data visualization",     "Statistical analysis",     "Neural networks",     "Deep learning",     "Natural language processing",     "Computer vision",     "Predictive modeling",     "Regression analysis",     "Clustering",     "Classification",     "Supervised learning",     "Unsupervised learning",     "Reinforcement learning",     "Decision trees",     "Random forests",     "Support vector machines",     "Dimensionality reduction",     "Feature engineering",     "Data cleaning",     "Data preprocessing",     "Data augmentation",     "Data integration",     "Data warehousing",     "Data governance",     "Data security",     "Data privacy",     "Cloud computing",     "Distributed systems",     "Parallel computing",     "High-performance computing",     "Algorithms",     "Computational complexity",     "Graph theory",     "Optimization",     "Numerical methods",     "Simulation",     "Data structures",     "Object-oriented programming",     "Functional programming",     "Programming languages",     "Software engineering",     "Agile methodologies",     "DevOps",     "Continuous integration",     "Continuous delivery",     "Version control"]
keywords = pd.DataFrame(keywords, columns=['keyword'])
keywords = keywords.reset_index().rename(columns={'index': 'id'})


# Save everything

# In[ ]:


import random
import string

def get_random_string(length=12):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def get_choice_from_keywords(keywords, n=5):
    return '|'.join(keywords.sample(n).keyword.tolist())


# In[ ]:


papers['doi'] = [get_random_string() for _ in range(len(papers))]


# In[ ]:


papers['keywords'] = [get_choice_from_keywords(keywords) for _ in range(len(papers))]
papers['keywords'] = replace_values_for_ids(papers, keywords, 'keywords', 'keyword')


# In[ ]:


# papers.columns = ['id', 'pages', 'title', 'author_ids', 'volume_id', 'edition_id', 'doi', 'keyword_ids', 'abstract']
# papers = papers[['id', 'doi', 'title', 'abstract', 'pages', 'author_ids', 'volume_id', 'edition_id', 'keyword_ids']]
# papers.to_csv('papers.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)


# In[ ]:


volumes.columns = ['id', 'volume', 'year', 'journal_id']
authors.columns = ['id', 'author']


# In[ ]:


correspondance = papers['author_ids'].apply(lambda x: get_correspondent_author(x))
correspondance = correspondance.reset_index().rename(columns={'index': 'id', 'author_ids': 'author_id'})
correspondance.head()


# In[ ]:


# citations.to_csv('citations.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
# editions.to_csv('editions.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
# volumes.to_csv('volumes.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
# journals.to_csv('journals.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
# conferences.to_csv('conferences.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
# authors.to_csv('authors.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
# correspondance.to_csv('corr_authors.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
# keywords.to_csv('keywords.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)


# Expanding the graph

# Reviewers

# In[ ]:


def get_paper_reviewers(authors, author_set, delim='|'):
    possible_reviewers = author_set - set(authors.split(delim))
    return '|'.join(np.random.choice(list(possible_reviewers), 3).astype(str))

reviewers = papers['author_ids'].apply(lambda x: get_paper_reviewers(x, author_set=set(authors['id'])))

reviewers = reviewers.reset_index().rename(columns={'index': 'id'})
reviewers.head()


# In[ ]:


# reviewers.to_csv('reviewers.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)


# Load reviews

# In[ ]:


def generate_review(generator, text='In this review, we suggest', max_length=100, num_return_sequences=1, seed=3):
    return generator(text, max_length=max_length, num_return_sequences=num_return_sequences)[0]['generated_text']

reviews = [generate_review(generator) for _ in range(20)]

def get_choice_from_reviews(reviews):
    return reviews[np.random.randint(len(reviews))]

reviews = [get_choice_from_abstracts(reviews) for _ in range(len(papers)*3)]

complete_reviews = pd.DataFrame(columns=['article_id', 'author_id', 'content', 'decision'])

content_id = 0
decision_dict = {
    0: 'Accepted',
    1: 'Accepted',
    2: 'Rejected'
}
for ix, reviewer_ids in reviewers.iterrows():
    for reviewer_id in reviewer_ids.author_ids.split('|'):
        complete_reviews = pd.concat([complete_reviews, pd.DataFrame([[ix, reviewer_id, reviews[content_id], decision_dict[content_id%3]]], columns=complete_reviews.columns)])
        content_id += 1


# In[ ]:


# complete_reviews.to_csv('reviews.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)


# Affiliations

# In[ ]:


affiliations = [    "Enron",    "Monsanto",    "Philip Morris",    "Lehman Brothers",    "Goldman Sachs",    "Halliburton",    "Blackwater",    "Uber",    "Facebook",    "Google",    "Amazon",    "Walmart",    "BP",    "ExxonMobil",    "Shell",    "Nestle",    "Nike",    "McDonald's",    "Coca-Cola",    "Pfizer",    "Johnson & Johnson",    "Volkswagen",    "Takata",    "Equifax",    "Yahoo"]

def get_choice_from_affiliations(affiliations):
    return affiliations[np.random.randint(len(affiliations))]

authors['affiliation'] = [get_choice_from_affiliations(affiliations) for _ in range(len(authors))]

author_affiliations = authors.drop('author:string', axis=1)
# author_affiliations.to_csv('affiliations.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)


# Injecting more data

# In[ ]:


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

# In[ ]:


# import numpy as np

# papers = pd.read_csv('data/papers.csv')
# keywords = pd.read_csv('data/keywords.csv')
# editions = pd.read_csv('data/editions.csv')

# keywords['keyword'] = keywords['keyword']
# data_keywords = keywords[keywords['keyword'].str.contains('Data|data')]['keyword'].to_list()

# def inject_keywords_to_conf(papers, editions, keywords, keyword_list, conference_id):
#     # Filter papers of conference
#     papers = papers.merge(editions, how='left', left_on='edition_id', right_on='id')
#     papers.rename({'id_x': 'id'}, inplace=True, axis=1)

#     # Get keyword ids (of interest)
#     keyword_dict = {v: k for k, v in keywords['keyword'].to_dict().items()}
#     keyword_ids = [keyword_dict[keyword] for keyword in keyword_list]

#     # Inject with random probability 
#     indices_list = []
#     keyword_ids_list =[]
#     for ix, row in papers.iterrows():
#         indices_list.append(ix)
#         if np.random.random() < 0.95 and row.conference_id == conference_id:
#             new_keyword_ids = row.keyword_ids + f'|{np.random.choice(keyword_ids)}'
#         else:
#             new_keyword_ids = row.keyword_ids
#         keyword_ids_list.append(new_keyword_ids)

#     return pd.DataFrame({
#         'id': indices_list,
#         'keyword_ids': keyword_ids_list
#     })

# df = inject_keywords_to_conf(papers, editions, keywords, data_keywords, 0)
# papers['keyword_ids'] = df['keyword_ids']
# papers.to_csv('data/papers.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)


# Affiliations

# In[ ]:


# import pandas as pd
# import csv
# import numpy as np

# orgs = pd.read_csv('data/affiliations.csv')
# orgs = pd.DataFrame(orgs['affiliation'].unique(), columns=['organization'])
# orgs = orgs.reset_index().rename({'index': 'id'}, axis=1)
# orgs['type'] = [np.random.choice(['C', 'U']) for _ in range(len(orgs))]
# # orgs.to_csv('data/organizations.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
# aff = pd.read_csv('data/affiliations.csv')
# affiliations = aff.merge(orgs, how='inner', left_on='affiliation', right_on='organization')\
# .rename({'id_x': 'id',
#          'id_y': 'organization_id'}, axis=1)\
# .drop(['affiliation', 'organization'], axis=1)

# # affiliations.to_csv('data/affiliations.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

