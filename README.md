# GraphChi-java
Version 0.2.2

## News

* Performance has been improved by parallelizing shard loading better (Oct 22, 2013)
* GraphChi's Java version has a new cool random walk simulation engine: https://github.com/GraphChi/graphchi-java/wiki/Personalized-Pagerank-with-DrunkardMob

 
# Introduction

Project for developing the Java version of GraphChi ( http://www.graphchi.org ), the disk-based graph computation engine. To learn more about GraphChi, visit the C++ version's project page: https://github.com/GraphChi/graphchi-cpp

**NEW:** GraphChi can be used in Hadoop/Pig scripts: [GraphChi for Pig](https://github.com/GraphChi/graphchi-java/wiki/GraphChi-For-Pig).

### How to use

 
Read the README.txt for information on how to build and run the example applications. You are going to need [Maven](http://maven.apache.org/download.cgi) or [sbt](http://www.scala-sbt.org/) for building.

graphchi-java is hosted in the maven central repository, so you can include it as a managed dependency in your maven or sbt builds.  For sbt, include the following line in your `build.sbt`:

`libraryDependencies += "org.graphchi" %% "graphchi-java" % "0.2.2"`

For maven, include the following in `<dependencies>`:

```
<dependency>
  <groupId>org.graphchi</groupId>
  <artifactId>graphchi-java_2.11</artifactId>
  <version>0.2.2</version>
</dependency>
```

It is a very good idea to study the example applications carefully. There are currently three example applications in the package **edu.cmu.graphchi.apps**:
* [PageRank](https://github.com/GraphChi/graphchi-java/tree/master/src/main/java/edu/cmu/graphchi/apps/Pagerank.java) for computing the famous [PageRank](http://en.wikipedia.org/wiki/PageRank) ranking
* [Connected Components](https://github.com/GraphChi/graphchi-java/tree/master/src/main/java/edu/cmu/graphchi/apps/ConnectedComponents.java) for computing the weakly connected components
* [Alternative Least Squares Matrix Factorization](https://github.com/GraphChi/graphchi-java/tree/master/src/main/java/edu/cmu/graphchi/apps/ALSMatrixFactorization.java)





### Input data

GraphChi-java supports [edge-list](https://github.com/GraphChi/graphchi-cpp/wiki/Edge-List-Format) and: [adjacency list](https://github.com/GraphChi/graphchi-cpp/wiki/Adjacency-List-Format) formats.

To preprocess the graph, you need to call "sharder" in the beginning of your program. For example, if you graph has floating-point input values for each edge, you can call it as follows:

```java
    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
            return new FastSharder<Float, Float>(graphName, numShards, new VertexProcessor<Float>() {
                public Float receiveVertexValue(int vertexId, String token) {
                    return (token == null ? 0.0f : Float.parseFloat(token));
                }
            }, new EdgeProcessor<Float>() {
                public Float receiveEdge(int from, int to, String token) {
                    return (token == null ? 0.0f : Float.parseFloat(token));
                }
            }, new FloatConverter(), new FloatConverter());
        }
    
        public static void main(String[] args) throws  Exception {
            String baseFilename = args[0];
            int nShards = Integer.parseInt(args[1]);
            String filetype= args[2]; // "edgelist" or "adjacency"
    
            /* Create shards */
            FastSharder sharder = createSharder(baseFilename, nShards);
            if (new File(ChiFilenames.getFilenameIntervals(baseFilename, nShards)).exists()) {
                    sharder.shard(new FileInputStream(new File(baseFilename)), filetype);
                } else {
                    logger.info("Found shards -- no need to preprocess");
                }
        ....
```

**Note:** (For edge-lists only) initial values for vertices are also now supported. If you define a self-edge (i.e edge with same source and destination), the value of the 'edge' is interpreted as value for the vertex. Thus you need to provide both a vertex-value and edge-value parser to the FastSharder.


### Hadoop / PIG

Since version 0.2 (January 2013), GraphChi Java programs can be directly invoked from [Pig](http://pig.apache.org). More information will on page GraphChiForPig.

Example PIG script:

```
    REGISTER graphchi-java-0.2-jar-with-dependencies.jar;
    
    pagerank = LOAD '/user/akyrola/graphs/soc-LiveJournal1.txt' USING edu.cmu.graphchi.apps.pig.PigPagerank as (vertex:int, rank:float);
    
    STORE pagerank INTO '/user/akyrola/pagerank';
```


### Scala

An early Scala-wrapper is also provided. See [PagerankScala](https://github.com/GraphChi/graphchi-java/blob/master/src/main/scala/edu/cmu/graphchi/scala/apps/PagerankScala.scala) for example.


### Differences to the C++ version

Following features are not implemented in the Java-version:
* dynamic graphs
* dynamic edge data

GraphChi implements a preprocessing step called "sharding", which reads an input graph and stores it in efficient binary format on the disk (see [Introduction to GraphChi](https://github.com/GraphChi/graphchi-cpp/wiki/Introduction-To-GraphChi) for more information). Java-version now includes its own sharding code (called "FastSharder"), which differs from the C++ version: FastSharder shuffles the order of vertices to guarantee (with high probability) an even distribution of edges over shards. Thus, internally GraphChi's Java-version uses different vertex IDs than in the original graph. 

Translating between the internal ids and original ids is easy using the **VertexIdTranslate** class:

```Java
         VertexIdTranslate trans = engine.getVertexIdTranslate();
         for(int i=0; i < engine.numVertices(); i++) {
              System.out.println("Internal id " + i + " = original id " + trans.backward(i));
          }
```


### Performance

On a new MacBook Pro (2012), I can run Pagerank on a Twitter-graph with 1.5 B edges (available from http://an.kaist.ac.kr/traces/WWW2010.html/)  in about 10 minutes / iteration.  It is 2-3x slower than the C++ version on an SSD disk.  I expect this performance to be sufficient for many users, especially researchers and analysts who do need real-time performance but favor a convenient programming environment.


## Acknowledgements

I want to thank Twitter and particularly the Personalization and Recommendations Team for the my internship during Fall 2012. M
any of the improvements to mature the GraphChi's Java-version were done during the course of the internship. 


## YourKit sponsorship

YourKit is kindly supporting GraphChi project with its full-featured Java Profiler. 
It is incredibly useful for debugging performance bottlenecks and analyzing memory usage.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and
[YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).


-- Aapo Kyrola, 
akyrola@cs.cmu.edu


