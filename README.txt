
GraphChi-java
Version 0.0.4  

BUILDING 

You can build GraphChi with any IDE, or using Ant. Just
write on the command-line:
    ant


RUNNING GRAPHCHI & PREPARING SHARDS

To run the pagerank example:
   java -Xmx4096m -cp build  edu.cmu.graphchi.apps.Pagerank [GRAPH-FILENAME] [NUM-OF-SHARDS]

or Connected Components:
   java -Xmx4096m -cp build  edu.cmu.graphchi.apps.ConnectedComponents [GRAPH-FILENAME] [NUM-OF-SHARDS]

Unfortunately, at this stage you need to use the C++ version to prepare shards. 
It is easy though:

1.  Get the C++ GraphChi from http://code.google.com/p/graphchi/

2.  Build the "sharder_basic" program:
    	  make sharder_basic

3.  Run the sharder:
    	  bin/sharder_basic file [GRAPH-FILENAME] nshards auto

The program will ask for the type of the edgedata. For pagerank,
enter "float". It will also ask for the input file type. Currently
edgelist and adjacency list are supported. For the description, see
http://code.google.com/p/graphchi/wiki/EdgeListFormat
http://code.google.com/p/graphchi/wiki/AdjacencyListFormat

Look for a message " Created x shards.", to see how many
shards it created. 

NOTE

I hope the code is readable enough, as documentation is not there
yet.


-- Aapo Kyrola, 
akyrola@cs.cmu.edu



