from neo4j import GraphDatabase, basic_auth
import configparser
import pandas as pd
import optparse

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))

def PageRank(session):
    query = """
    CALL gds.pagerank.stream('Paper', 'Cite', {iterations:20, dampingFactor:0.85})
    YIELD nodeId, score
    RETURN algo.getNodeById(nodeId).title AS paper,score
    ORDER BY score DESC
    """
    session.run(query)

    
def coauthor_SCC(session):
    query = """
    CALL gds.alpha.scc.stream('Author', 'CoauthorWith', {})
    YIELD nodeId, community
    RETURN community, COLLECT(algo.getNodeById(nodeId).name) AS authorList
    """
    session.run(query)


with driver.session() as session:
    PageRank(session)
    coauthor_SCC(session)
