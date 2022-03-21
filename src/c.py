from neo4j import GraphDatabase, basic_auth
import configparser
import pandas as pd
import optparse

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
    }
    """
    session.run(query)

    
def pageRank(session):
    query = """
    CALL gds.pageRank.stream('myGraph')
    YIELD nodeId, score
    RETURN gds.util.asNode(nodeId).title AS paper,score
    ORDER BY score DESC
    """
    session.run(query)

    
def triangleCount(session):
    query = """
    CALL gds.triangleCount.stream('myGraph2')
    YIELD nodeId, triangleCount
    RETURN gds.util.asNode(nodeId).title AS title, triangleCount
    ORDER BY triangleCount DESC
    """
    session.run(query)


with driver.session() as session:
    create_graph(session)
    pageRank(session)
    create_graph2(session)
    triangleCount(session)
