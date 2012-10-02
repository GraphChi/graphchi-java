package com.twitter.graphchi.topic_pagerank

import edu.cmu.graphchi.ChiFilenames
import edu.cmu.graphchi.datablocks.IntConverter
import edu.cmu.graphchi.engine.auxdata.VertexData
import edu.cmu.graphchi.scala._
import edu.cmu.graphchi.util.{HugeFloatMatrix, Toplist}
import java.io._
import scala.collection.JavaConversions._
import scala.io.Source


case class SumAndNormalizer(sum: java.lang.Float, normalizer: java.lang.Float) {
  def +(that: SumAndNormalizer) = SumAndNormalizer(this.sum + that.sum, this.normalizer + that.normalizer)
}

/**
 * Computes personalized pagerank for a several "topics" a time.
 * Input: a list of files containing normalized weights for each topic.
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
object WeightedPersonalizedPagerank {

  val RESETPROB = 0.15f

  var weights : HugeFloatMatrix = null

  case class TopicInfo(topicName: String, weightFile: String)

  var topicInfos : Array[TopicInfo] = null


  def initialize(initfile: String) : Int = {
    // files have to be in the same directory
    val dir = new File(new File(initfile).getAbsolutePath()).getParentFile()
    topicInfos = Source.fromFile(initfile).getLines().filter(_.indexOf("\t") > 0).map( line  => {
      val toks = line.split("\t")
      TopicInfo(toks(0), dir.getAbsolutePath() + "/" + toks(1))
    }).toArray
    println(topicInfos)
    topicInfos.size
  }

  // Initialize topic-weights
  def initVertexData[T](graphname: String, graphchiSqr: GraphChiSquared[T]) {
    val vertexDataFile = new File(ChiFilenames.getFilenameOfVertexData(graphname, new IntConverter()))
    if (vertexDataFile.exists()) vertexDataFile.delete()

    val vertexVals = VertexData.createIntArray(graphchiSqr.numVertices())
    weights  = new HugeFloatMatrix(graphchiSqr.numVertices(), topicInfos.length)
    // Clean up....
    topicInfos.zip((0 until topicInfos.size)).foreach( x => { val (topicInfo, compidx) = x
      Source.fromFile(topicInfo.weightFile).getLines().filter(_.length > 0)foreach(line => {
        val toks = line.split("\t")
        val vertexId = Integer.parseInt(toks(0))
        val weight = java.lang.Float.parseFloat(toks(2))
        if (vertexId < vertexVals.length) {   // Graph and the topic-seeds might be out of sync
          weights.setValue(vertexId, compidx, weight)
        } else println("Warning: too large vertex-id in topic top:", vertexId)
      })
    })

  }





  def main(args: Array[String]): Unit = {
    val graphname = args(0)
    val nshards = Integer.parseInt(args(1))
    val niters = Integer.parseInt(args(2))
    val initfile = args(3)

    val nComputations = initialize(initfile)
    val graphchiSqr = new GraphChiSquared[SumAndNormalizer](graphname, nshards, nComputations)

    /* Ensure that we can run many processes in parallel */
    ChiFilenames.vertexDataSuffix = ".weighted." + ChiFilenames.getPid

    println("Initializing... Starting ", nComputations, " computations on WEIGHTED personalized pagerank.")
    initVertexData(graphname, graphchiSqr)
    println("Done initializing")


    /* Compute */
    graphchiSqr.compute(niters,
      gatherInit = SumAndNormalizer(0.0f, 0.0f),
      gather =  (v, nbrId, neighborVal, gather, compid) => {
        val w = weights.getValue(nbrId, compid)
        gather + SumAndNormalizer(neighborVal * w, w)
      },
          apply = (v, gather, compid) =>
              if (gather.normalizer > 0)
                  (RESETPROB * weights.getValue(v.id(), compid) +
                      (1 - RESETPROB) * gather.sum) / gather.normalizer
              else 0.0f,
          vertexFilter = (v => v.numOutEdges() > 0)
        )

    println("Ready, writing toplists...")

    /* Output top-lists */
    val ntop = 10000;
    (0 until nComputations).foreach(icomp => {
      val topList = Toplist.topList(graphchiSqr.getVertexMatrix(), icomp, ntop)
      val outputfile = "toplist.weightedPR." + topicInfos(icomp).topicName + ".tsv"

      val writer = new BufferedWriter(new FileWriter(new File(outputfile)));
      topList.foreach( item => writer.write(item.getVertexId + "\t" + topicInfos(icomp).topicName + "\t" + item.getValue +"\n"))

      writer.close()
    })

    // Delete vertex data file
    new File(ChiFilenames.getFilenameOfVertexData(graphname, new IntConverter())).delete()
  }
}