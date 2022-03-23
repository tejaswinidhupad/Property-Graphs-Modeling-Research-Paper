from neo4j import GraphDatabase, basic_auth
import pandas as pd

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))

def top_three_most_cited(session):
    query = """
        MATCH (citingPaper:Paper)-[citation:Cite]->(citedPaper:Paper)-[:published]-(proceeding:Proceeding)
        WITH COUNT(citation) AS citedCount, citedPaper, proceeding ORDER BY citedCount DESC
        WITH COLLECT(citedPaper.title) AS mostCitedPaper, proceeding
        RETURN proceeding.title as conference, mostCitedPaper[0..3] as top_3_most_cited_papers
    """
    return pd.DataFrame([(record['conference'], record['top_3_most_cited_papers']) for record in session.run(query)], columns=['conference','top_3_papers'])

def published_four_different_edition(session):
    query = """
        MATCH (a:Author)-[:Write]->(pa:Paper)-[pi:published]->(p:Proceeding)
        with a, p, count(pa.key) as no_edition
        where p.key =~ 'conf.*'
        and no_edition>=4
        return a.name as author_name,p.key as conference, no_edition 
    """
    return pd.DataFrame([(record['author_name'], record['conference'], record['no_edition']) for record in session.run(query)], columns=['authorname','conf_name','no_of_edition'])

def impact_factor(session):
    query = """
        MATCH (p1:Paper)-[:published]->(j1:Journal)
        WHERE toInteger(j1.year)=(2000-1) OR toInteger(j1.year)= (2000-2)
        WITH j1.name as journal_name, size(COLLECT(p1)) AS nop, COLLECT(p1) AS c_journal
        MATCH(p1:Paper)-[c1:Cite]->(p2:Paper)
        WHERE p1 IN c_journal
        RETURN journal_name, (toFloat(COUNT(c1))/nop) AS impact_factor ORDER BY impact_factor DESC
    """
    return pd.DataFrame([(record['journal_name'], record['impact_factor']) for record in session.run(query)], columns=['JournalName','ImpactFactor'])

def h_index(session):
    query = """
        match (a:Author)-[w:Write]->(p:Paper)-[c:Cite]->(p1:Paper)
        with a,p1,collect([id(p),p.title]) as rows
        WITH a,p1,RANGE(1,SIZE(rows))AS enumerated_rows
        UNWIND enumerated_rows AS er
        with a,er AS rank,count(p1) as cited
        where rank <= cited 
        return a.name as author_name, rank as h_index
        order by h_index desc
    """
    return pd.DataFrame([(record['author_name'], record['h_index']) for record in session.run(query)], columns=['authorname','hindex'])


with driver.session() as session:
    print('1')
    print(top_three_most_cited(session))
    print('2')
    print(published_four_different_edition(session))
    print('3')
    print(impact_factor(session))
    print('4')
    print(h_index(session))