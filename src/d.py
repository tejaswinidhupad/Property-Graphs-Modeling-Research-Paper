from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))


def community(session):
    query = """
    MATCH (dbKeyword:Keyword)<-[:has]-(paperAboutKeywords:Paper)-[:published]->(publication)-[:inBook]->(book:Book)
    WHERE dbKeyword.keyword in ["data", "management", "indexing", "modeling", "big", "processing", "storage", "querying"] or dbKeyword.keyword =~ '.*data.*'
    WITH book, COUNT(DISTINCT paperAboutKeywords) AS nPaperAboutKeywords 
    MATCH (book)<-[:inBook]-(publication)<-[:published]-(paperInBook:Paper)
    WITH book,nPaperAboutKeywords,COUNT(paperInBook) as nPaperInBook
    WITH book,nPaperAboutKeywords,nPaperInBook,(tofloat(nPaperAboutKeywords)/nPaperInBook) as percentagepaperAboutKeywords 
    WHERE percentagepaperAboutKeywords > 0.3
    WITH book.title as booktitle, nPaperAboutKeywords, nPaperInBook, percentagepaperAboutKeywords
    order by percentagepaperAboutKeywords desc
    RETURN collect(booktitle)
    """
    return session.run(query).single()[0]


def top_100(session, conferences):
    query = """
	CALL gds.pageRank.stream('myGraph')
    YIELD nodeId, score
    WITH gds.util.asNode(nodeId) AS paper,score
    MATCH (book:Book)<-[:inBook]-(:Proceeding)<-[:published]-(paper)
    WHERE book.title IN """+str(conferences)+""" 
    WITH paper.title as papertitle, book.title as booktitle, score ORDER BY score DESC
    RETURN COLLECT(papertitle)
    LIMIT 100
    """
    return session.run(query).single()[0]


def get_gurus(session, top_papers):
    query = """
    MATCH (paper:Paper)<-[:Write]-(author:Author)
    WHERE paper.title IN """+str(top_papers)+"""
    WITH author.name as author, COUNT(paper) AS paperCount
    WHERE paperCount >= 2
    RETURN COLLECT(author)
    """
    return session.run(query).single()[0]

def execute(session):
    aa = community(session)
    print('community')
    print(aa)
    bb = top_100(session, aa)
    print('top_100')
    print(bb)
    db_gurus = get_gurus(session,bb)

    return db_gurus

if __name__ == '__main__':
    with driver.session() as session: 
        gurus = execute(session)
        print('Answer')
        print(gurus)



