
# GraphChi-java
Version 0.2


## News

* GraphChi was moved to GitHub from Google Code (July 24). Please report/fix any broken links.
* GraphChi's Java version has a new cool random walk simulation engine: https://github.com/GraphChi/graphchi-java/wiki/Personalized-Pagerank-with-DrunkardMob

## Building 

You can build GraphChi with any IDE, or using Maven. Just
write on the command-line:
     mvn assembly:assembly -DdescriptorId=jar-with-dependencies


## Running GraphChi

To run the pagerank example:
   java -Xmx4096m -cp target/graphchi-java-0.2-jar-with-dependencies.jar  edu.cmu.graphchi.apps.Pagerank [GRAPH-FILENAME] [NUM-OF-SHARDS] [FILETYPE]

or Connected Components:
   java -Xmx4096m -cp target/graphchi-java-0.2-jar-with-dependencies.jar  edu.cmu.graphchi.apps.ConnectedComponents [GRAPH-FILENAME] [NUM-OF-SHARDS] [FILETYPE]

Above, FILETYPE can be "edgelist" or "adjlist". See https://github.com/GraphChi/graphchi-cpp/wiki/Edge-List-Format and https://github.com/GraphChi/graphchi-cpp/wiki/Adjacency-List-Format for descriptions.





-- Aapo Kyrola, 
akyrola@cs.cmu.edu



