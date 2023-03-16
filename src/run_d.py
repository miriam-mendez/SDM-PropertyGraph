from connection import Neo4jConnection
import argparse
import time

if __name__ == "__main__":
    # Parse data to build the connection
    parser = argparse.ArgumentParser()
    parser.add_argument("-uri", "--uri",
                   help="Neo4j URI",
                   type=str, default="bolt://localhost:7687")
    parser.add_argument("-usr", "--username",
                   help="Auth username",
                   type=str, default="neo4j")
    parser.add_argument("-pwd", "--password",
                   help="Auth password",
                   type=str)
    parser.add_argument("-db", "--database",
                   help="Database to connect to",
                   type=str)
    args = parser.parse_args()

    # Connection
    conn = Neo4jConnection(args.uri, args.username, args.password, args.database)

    # Run queries from D: d1 (node similarity) d2 (louvain)
    files = ['d1_1','d1_2','d2_1','d2_2','d2_3']
    conn.query("CALL gds.graph.drop('bigraph', false) YIELD graphName;")
    conn.query("CALL gds.graph.drop('graph', false) YIELD graphName;")
            
    for file in files:
        print(file)
        with open(f'./cypher/{file}.cypher', 'r') as f:
            query = f.read()
            results = conn.query(query, with_results=True)
            print(results)
            time.sleep(2)
