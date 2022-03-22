from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))

def top_three_most_cited(session):
    query = """
        MATCH (citingPaper:Paper)-[citation:Cite]->(citedPaper:Paper)-[:published]-(proceeding:Proceeding)
        WITH COUNT(citation) AS citedCount, citedPaper, proceeding ORDER BY citedCount DESC
        WITH COLLECT(citedPaper.title) AS mostCitedPaper, proceeding
        RETURN proceeding.title as conference, mostCitedPaper[0..3] as top_3_most_cited_papers
    """
    session.run(query)

def published_four_different_edition(session):
    query = """
        MATCH (a:Author)-[:Write]->(pa:Paper)-[pi:published]->(p:Proceeding)
        with a, p, count(pa.key) as no_edition
        where p.key =~ 'conf.*'
        and no_edition>=4
        return a.name as author_name,p.key as conference, no_edition 
    """
    session.run(query)

def impact_factor(session):
    query = """
        MATCH (p1:Paper)-[:published]->(j1:Journal)
        WHERE toInteger(j1.year)=(2000-1) OR toInteger(j1.year)= (2000-2)
        WITH j1.name as JournalName, size(COLLECT(p1)) AS nop, COLLECT(p1) AS c_journal
        MATCH(p1:Paper)-[c1:Cite]->(p2:Paper)
        WHERE p1 IN c_journal
        RETURN JournalName, (toFloat(COUNT(c1))/nop) AS ImpactFactor ORDER BY ImpactFactor DESC
    """
    session.run(query)
    
def h_index(session):
    query = """
        match (a:Author)-[w:Write]->(p:Paper)-[c:Cite]->(p1:Paper)
        with a,p1,collect([id(p),p.title]) as rows
        WITH a,p1,RANGE(1,SIZE(rows))AS enumerated_rows
        UNWIND enumerated_rows AS er
        with a,er AS rank,count(p1) as cited
        where rank <= cited 
        return a.name as authorname, rank as hindex
        order by hindex desc
    """
    session.run(query)


with driver.session() as session:
    top_three_most_cited(session)
    published_four_different_edition(session)
    impact_factor(session)
    h_index(session)