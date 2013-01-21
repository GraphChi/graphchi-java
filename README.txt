
GraphChi-java
Version 0.2

BUILDING 

You can build GraphChi with any IDE, or using Ant. Just
write on the command-line:
    mvn package


RUNNING GRAPHCHI

To run the pagerank example:
   java -Xmx4096m -cp target/graphchi-java-0.2.jar  edu.cmu.graphchi.apps.Pagerank [GRAPH-FILENAME] [NUM-OF-SHARDS] [FILETYPE]

or Connected Components:
   java -Xmx4096m -cp target/graphchi-java-0.2.jar  edu.cmu.graphchi.apps.ConnectedComponents [GRAPH-FILENAME] [NUM-OF-SHARDS] [FILETYPE]

Above, FILETYPE can be "edgelist" or "adjlist". See http://code.google.com/p/graphchi/wiki/EdgeListFormat and http://code.google.com/p/graphchi/wiki/AdjacencyListFormat for descriptions.





-- Aapo Kyrola, 
akyrola@cs.cmu.edu



