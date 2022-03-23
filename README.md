# Property-Graphs-Modeling-Research-Paper
This is a hands-on lab on Property Graphs. We will be using what is, most probably, the most popular property graph database: Neo4j. We will practice how to create and manage graphs, as well as querying/processing them.

# To run the code
Create a noe4j service on "localhost:7687" and set passward as "123".
Install Graph Data Science Library.

## Instantiating/Loading
The data was used from the official website of <a href="https://dblp.org/faq/16154937.html">DBLP</a>.The dataset was in the XML format, hence was needed to convert to CSV format for loading into Neo4j. The CSV files were generated by running the below command :

<code>XMLToCSV.py xml_filename dtd_filename outputfile</code>

The data in the CSV is explained as below :<br>
1. article – An article from a journal or magazine.<br>
2. inproceedings – A paper in a conference or workshop proceedings.<br>
3. proceedings – The proceedings volume of a conference or workshop.<br>
4. book – An authored monograph or an edited collection of articles.<br>
5. incollection – A part or chapter in a monograph.<br>
6. phdthesis – A PhD thesis.<br>
7. mastersthesis – A Master's thesis. There are only very few Master's theses in dblp.<br>
8. www – A web page. There are only very few web pages in dblp.<br>



