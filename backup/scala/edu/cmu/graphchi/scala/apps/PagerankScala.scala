package edu.cmu.graphchi.scala.apps

import edu.cmu.graphchi.scala._
import edu.cmu.graphchi.datablocks.FloatConverter
import edu.cmu.graphchi.util.IdFloat
import edu.cmu.graphchi.util.Toplist
import java.util.TreeSet
import scala.collection.JavaConversions._
import edu.cmu.graphchi.preprocessing.{EdgeProcessor, VertexProcessor, FastSharder}
import java.io.FileInputStream

/**
 * Scala version of Pagerank. For illustration only --- needs cleanup.
 */
object PagerankScala {

  def main(args: Array[String]): Unit = {
    val filename = args(0)
    val nshards = args(1).toInt
    val filetype = args(2)
    val niters = 4

    /* Preprocessing */
    val sharder = new FastSharder(filename, nshards,   new VertexProcessor[java.lang.Float] {
      def receiveVertexValue(vertexId: Int, token: String) = 1.0f
    }, new EdgeProcessor[java.lang.Float] {
      def receiveEdge(from: Int, to: Int, token: String) = 1.0f
    }, new FloatConverter(), new FloatConverter())
    sharder.shard(new FileInputStream(filename), filetype)

    /* Run GraphChi */
    val graphchi = new GraphChiScala[java.lang.Float, java.lang.Float, java.lang.Float](filename, nshards)
    graphchi.setEdataConverter(new FloatConverter())
    graphchi.setVertexDataConverter(new FloatConverter())

    /* Notice different interface compared to the java version */
    graphchi.initializeVertices(v => 1.0f)
    graphchi.foreach(niters,
      gatherDirection = INEDGES(),
      gatherInit = 0.0f,
      gather =  (v, edgeval, iter, gather) => gather + edgeval,
      apply = (gather, v) => 0.15f + 0.85f * gather,
      scatterDirection = OUTEDGES(),
      scatter = (v) => v.value() / v.outDegree
    )

    /* Print top (this is just Java code)*/
    val top20 = Toplist.topListFloat(filename, graphchi.numVertices, 20);
    var i : Int = 0;
    top20.foreach( vertexRank => {
      i = i + 1
      System.out.println(i + ": " +
        graphchi.vertexTranslate.backward(vertexRank.getVertexId()) + " = " + vertexRank.getValue());
    } )
  }
}