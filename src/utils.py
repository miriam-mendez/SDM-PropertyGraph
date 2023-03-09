#!/usr/bin/env python3
"""
Dumb utils for data.
"""
import math
import numpy as np
import networkx as nx
import pandas as pd
from pathlib import Path

DATA_PATH = Path('../data')

class RawData:
    def __init__(self, data_path) -> None:
        self.articles = pd.read_csv(data_path/'articles.csv')
        self.authors = pd.read_csv(data_path/'authors.csv')
        self.editions = pd.read_csv(data_path/'editions.csv')
        self.volumes = pd.read_csv(data_path/'volumes.csv')
        self.conferences = pd.read_csv(data_path/'conferences.csv')
        self.journals = pd.read_csv(data_path/'journals.csv')
        self.keywords = pd.read_csv(data_path/'keywords.csv')

    def complete_join(self):
        df = self.articles.merge(self.editions, how='left', left_on='edition_key', right_on='key', suffixes=('', '_edition')).drop('key_edition', axis=1)\
        .merge(self.conferences, how='left', left_on='conference_key', right_on='key', suffixes=('', '_conference')).drop('key_conference', axis=1)\
        .merge(self.volumes, how='left', left_on='volume_key', right_on='key', suffixes=('', '_volume')).drop('key_volume', axis=1)\
        .merge(self.journals, how='left', left_on='journal_key', right_on='key', suffixes=('', '_journal')).drop('key_journal', axis=1)\
        .drop(['edition_key', 'volume_key', 'conference_key', 'journal_key'], axis=1)

        df = df.assign(year=df['year'].mask(df['year'].isna(), df['year_volume'])).drop('year_volume', axis=1)
        return df
    

def generate_citations(df, alpha=2):
    max_year = df['year'].max()
    citing_articles = []
    cited_articles = []
    for ix, article in df.iterrows():
        n_possible_cited = np.random.randint(0, math.ceil((max_year-article.year)*alpha+0.01))
        
        for ix, cited in df.sample(n=n_possible_cited).iterrows():
            if article.year > cited.year:
                citing_articles.append(article.key)
                cited_articles.append(cited.key)

    citations = pd.DataFrame({'citer_key': citing_articles, 
                            'cited_key': cited_articles})
    return citations


def check_citations_is_dag(citations):
    G = nx.DiGraph()

    for ix, row in citations.iterrows():
        G.add_edge(row.citer_key, row.cited_key)

    return nx.is_directed_acyclic_graph(G)