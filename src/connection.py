from neo4j import GraphDatabase

class Neo4jConnection:

    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    def query(self, query, with_results=False):
        with self.driver.session(database=self.database) as session:
            if not with_results:
                response = session.run(query)
            else:
                response = session.execute_read(self._query_read, query)
        return response
    
    def _query_read(self, tx, query):
        result = tx.run(query)
        return result.data()