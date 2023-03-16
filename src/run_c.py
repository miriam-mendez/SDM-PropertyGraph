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

    # Run queries from C: Recommender
    files = ['c1', 'c2', 'c3_1', 'c3_2', 'c4']
    
    for file in files[:-1]:
        print(file)
        with open(f'./cypher/{file}.cypher', 'r') as f:
            query = f.read()
            conn.query(query)
            time.sleep(1)
    
    with open(f'./cypher/c4.cypher', 'r') as f:
        query = f.read()
        results = conn.query(query, with_results=True)
        print(results)
