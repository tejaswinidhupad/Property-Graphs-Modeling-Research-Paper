from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))

def create_reviewed_edge(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
        AS tr
        MATCH (paper:Paper {key:tr.key})
        UNWIND SPLIT(tr.reviewer,"|") as reviewer
        MATCH (author:Author {name:reviewer})
        MERGE (paper)-[:reviewed]->(author)
    """
    session.run(query)

def assign_author_affiliation(session):
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///affiliation_of_authors.csv' AS ROW
    MATCH (a:Author {name:ROW.author})
    SET a.affiliation = ROW.affiliation
    """
    session.run(query)

def assign_suggestion(session):
    query = """
        MATCH (p:Paper)-[r:reviewed]-(a:Author)
        WITH r,(rand()) AS acceptanceProbability
        WITH r, acceptanceProbability,
        CASE
        WHEN acceptanceProbability>0.5 THEN True
        ELSE False END AS suggestion
        SET r.suggestion=suggestion
    """
    session.run(query)


if __name__ == '__main__':
    with driver.session() as session: 
        create_reviewed_edge(session)
        assign_author_affiliation(session)
        assign_suggestion(session)




