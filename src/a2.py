from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123"))


#delete
def delete_all_node(session): 
    query = "MATCH (n) DELETE n"
    session.run(query)

def delete_all_edge(session): 
    query = "MATCH ()-[r]-() DELETE r"
    session.run(query)
    
#node
def create_paper_node(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
        AS tr
        CREATE (paper:Paper 
        {ee:tr.ee, journal:tr.journal, key:tr.key, mdate:tr.mdate, 
        pages:tr.pages, title:tr.title, type:tr.type, cite:tr.cite, crossref:tr.crossref, 
        reviewer:tr.reviewer, keyword:tr.keyword});
    """
    session.run(query)
    
def create_author_node(session):
    query = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS FROM "file:///authors.csv" 
        AS tr
        CREATE (:Author {name:tr.author, key:tr.key})
    """
    session.run(query)

def create_book_node(session):
    query = """
        MATCH (proceeding:Proceeding)
        WITH DISTINCT proceeding.booktitle as booktitles
        UNWIND booktitles as title
        CREATE (:Book {title:title})
    """
    session.run(query)

def create_proceeding_node(session):
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///proceedings.csv' AS tr
    CREATE (:Proceeding {series:tr.series, location:tr.location,
    mdate:tr.mdate,year:tr.year,key:tr.key,editor:tr.editor,
    publisher:tr.publisher,isbn:tr.isbn,booktitle:tr.booktitle,
    title:tr.title,ee:tr.ee,volume:tr.volume})
    """    
    session.run(query)
    
def create_keyword_node(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///keywords.csv' 
        AS tr
        CREATE (:Keyword {keyword:tr.keyword})
    """
    session.run(query)

def create_journal_node(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///journals.csv' 
        AS tr
        CREATE (:Journal {name:tr.journal, volume:tr.volume, year:tr.year})
    """
    session.run(query)
    
def create_keyword_node(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///keywords.csv' 
        AS tr
        CREATE (:Keyword {keyword:tr.keyword})
    """
    session.run(query)

def create_journal_node(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///journals.csv' 
        AS tr
        CREATE (:Journal {name:tr.journal, volume:tr.volume, year:tr.year})
    """
    session.run(query)
    
#edge   
def create_published_journal_edge(session):
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
    AS tr
    MATCH (journal:Journal {name:tr.journal, year:tr.year,volume:tr.volume})
    MATCH (paper:Paper {key:tr.key})
    MERGE (paper)-[:published]->(journal)
    """
    session.run(query)
    
def create_published_in_proceeding_edge(session):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
        AS tr
        MATCH (proceeding:Proceeding {key:tr.crossref})
        MATCH (paper:Paper {key:tr.key})
        MERGE (paper)-[:published]->(proceeding)
    """
    session.run(query)
    
def create_cite_edge(session):
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
    AS tr
    UNWIND SPLIT(tr.cite,"|") as citedPaperKey
    MATCH (citingPaper:Paper {key:tr.key})
    MATCH (citedPaper:Paper {key:citedPaperKey})
    MERGE (citingPaper)-[:Cite]->(citedPaper)
    """
    session.run(query)

def create_has_keyword_edge(session):
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///papers.csv' 
    AS tr
    UNWIND SPLIT(tr.keyword,"|") as keyword
    MATCH (paper:Paper {key:tr.key})
    MATCH (usedKeyword:Keyword {keyword:keyword})
    MERGE (paper)-[:has]-(usedKeyword)
    """
    session.run(query)

def create_part_of_edge(session):
    query = """
    MATCH (p:Proceeding)
    WITH p
    MATCH (b:Book {title:p.booktitle})
    MERGE (p)-[:inBook]-(b)
    """
    session.run(query)
    
def create_write_edge(session): 
    query = """
        MATCH (author:Author) 
        UNWIND SPLIT(author.key,"|") as key
        MATCH (paper:Paper {key:key})
        MERGE (author)-[:Write]->(paper);
    """
    session.run(query)
    
def create_index_on_paper_key(session):
    query = "CREATE INDEX ON :Paper(key)"
    session.run(query)
    
def drop_index_on_paper_key(session):
    query = "DROP INDEX ON :Paper(key)"
    session.run(query)

def create_index_on_author_name(session):
    query = "CREATE INDEX ON :Author(name)"
    session.run(query)

def drop_index_on_author_name(session):
    query = "DROP INDEX ON :Author(name)"
    session.run(query)

def create_index_on_keyword(session):
    query = "CREATE INDEX ON :Keyword(keyword)"
    session.run(query)

def drop_index_on_keyword(session):
    query = "DROP INDEX ON :Keyword(keyword)"
    session.run(query)

def create_index_on_journal(session):
    query = "CREATE INDEX ON :Journal(name)"
    session.run(query)
    
def drop_index_on_journal(session):
    query = "DROP INDEX ON :Journal(name)"
    session.run(query)

def create_index_on_proceeding(session):
    query = "CREATE INDEX ON :Proceeding(key)"
    session.run(query)
    
def drop_index_on_proceeding(session):
    query = "DROP INDEX ON :Proceeding(key)"
    session.run(query)

def create_index_on_book(session):
    query = "CREATE INDEX ON :Book(title)"
    session.run(query)
    
def drop_index_on_book(session):
    query = "DROP INDEX ON :Book(title)"
    session.run(query)


with driver.session() as session:
    delete_all_edge(session)
    delete_all_node(session)    
    
    '''
    drop_index_on_paper_key(session)
    drop_index_on_author_name(session)
    drop_index_on_keyword(session)
    drop_index_on_book(session)
    drop_index_on_proceeding(session)
    drop_index_on_journal(session)
    '''
    create_proceeding_node(session)
    create_journal_node(session)
    create_keyword_node(session)
    create_paper_node(session)
    create_author_node(session)
    create_book_node(session)
    
    create_index_on_journal(session)
    create_index_on_paper_key(session)
    create_index_on_author_name(session)
    create_index_on_keyword(session)
    create_index_on_proceeding(session)
    create_index_on_book(session)

    create_write_edge(session)
    create_published_journal_edge(session)
    create_cite_edge(session)
    create_has_keyword_edge(session)
    create_published_in_proceeding_edge(session)
    create_part_of_edge(session)
    