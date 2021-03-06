from neo4j import GraphDatabase, basic_auth
import pandas as pd

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))

def create_graph(session):
    query = """
    CALL gds.graph.create('myGraph','Paper','Cite')
    """
    session.run(query)
    
def create_graph2(session):
    query = """  
    CALL gds.graph.create('myGraph2','Paper',{
        Cite: {
            orientation: 'UNDIRECTED'
        }
    })
    """
    session.run(query)

    
def pageRank(session):
    query = """
    CALL gds.pageRank.stream('myGraph')
    YIELD nodeId, score
    RETURN gds.util.asNode(nodeId).title AS paper,score
    ORDER BY score DESC
    """
    return pd.DataFrame([(record['paper'], record['score']) for record in session.run(query)], columns=['paper','score'])

    
def triangleCount(session):
    query = """
    CALL gds.triangleCount.stream('myGraph2')
    YIELD nodeId, triangleCount
    RETURN gds.util.asNode(nodeId).title AS title, triangleCount
    ORDER BY triangleCount DESC
    """
    return pd.DataFrame([(record['title'], record['triangleCount']) for record in session.run(query)], columns=['community','authorList'])


with driver.session() as session:
    create_graph(session)
    print('1')
    print(pageRank(session))
    create_graph2(session)
    print('2')
    print(triangleCount(session))
