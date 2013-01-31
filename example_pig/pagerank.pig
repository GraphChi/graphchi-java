
# This example shows how to run GraphChi programs inside Pig-scripts for Hadoop (http://pig.apache.org).
# The live-journal graph used in the example can be downloaded from
#  http://snap.stanford.edu/data/soc-LiveJournal1.html
#
#  Author: Aapo Kyrola, akyrola@cs.cmu.edu
#  Acknowledgements: Pig-integration was developed during author's internship at Twitter, Fall 2012.
#  Special thanks to Pankaj Gupta and Dong Wang.


# You need to create the jar-file first (see README.txt)
REGISTER graphchi-java-0.2-jar-with-dependencies.jar;

# Now, use the special PigPagerank -class as a loader to "load" the graph.
# In addition to loading, this actually performs the computation.
# The graph must be stored in HDFS.
pagerank = LOAD 'soc-LiveJournal1.txt' USING edu.cmu.graphchi.apps.pig.PigPagerank as (vertex:int, rank:float);

# Now store the result into HDFS.
STORE pagerank INTO 'pagerank_livejournal';

# Done!

