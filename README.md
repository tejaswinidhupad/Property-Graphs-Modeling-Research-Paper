# Property-Graphs-Modeling-Research-Paper
This is a hands-on lab on Property Graphs. We will be using what is, most probably, the most popular property graph database: Neo4j. We will practice how to create and manage graphs, as well as querying/processing them.

## Instantiating/Loading
The data was used from the official website of DBLP "<a href="https://dblp.org/faq/16154937.html">".The dataset was in the XML format, hence was needed to convert to CSV format for loading into Neo4j. The CSV files were generated by running the below command :

<code>XMLToCSV.py xml_filename dtd_filename outputfile</code>

The data in the CSV is explained as below :
article – An article from a journal or magazine.
inproceedings – A paper in a conference or workshop proceedings.
proceedings – The proceedings volume of a conference or workshop.
book – An authored monograph or an edited collection of articles.
incollection – A part or chapter in a monograph.
phdthesis – A PhD thesis.
mastersthesis – A Master's thesis. There are only very few Master's theses in dblp.
www – A web page. There are only very few web pages in dblp



