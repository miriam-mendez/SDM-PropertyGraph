from neo4j import GraphDatabase

class Neo4jConnection:

    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    def query(self, query, get_results=False):
        with self.driver.session(database=self.database) as session:
            if not get_results:
                response = session.run(query)
            else:
                response = session.read_transaction(query)
        
        return response

    # def print_greeting(self, message):
    #     with self.driver.session() as session:
    #         greeting = session.execute_write(self._create_and_return_greeting, message)
    #         print(greeting)

    # @staticmethod
    # def _create_and_return_greeting(tx, message):
    #     result = tx.run("CREATE (a:Greeting) "
    #                     "SET a.message = $message "
    #                     "RETURN a.message + ', from node ' + id(a)", message=message)
    #     return result.single()[0]