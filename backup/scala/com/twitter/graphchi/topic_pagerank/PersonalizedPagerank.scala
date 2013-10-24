package com.twitter.graphchi.topic_pagerank

import edu.cmu.graphchi.scala._
import edu.cmu.graphchi.datablocks.{IntConverter, FloatConverter}
import edu.cmu.graphchi.util.IdFloat
import edu.cmu.graphchi.util.Toplist
import java.util.TreeSet
import scala.collection.JavaConversions._
import java.util.Random
import scala.io.Source
import edu.cmu.graphchi.ChiFilenames
import java.io._
import edu.cmu.graphchi.engine.auxdata.{DegreeData, VertexData}

/**
 * Computes personalized pagerank for a several "topics" a time.
 * Input: a list of files containing list of vertex-ids with non-zero reset probability
 * for a topic. TODO: improve doc.
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
object PersonalizedPagerank {

  val RESETPROB = 0.15f

  case class TopicInfo(topicName: String, seedFile: String)

  var topicInfos : Array[TopicInfo] = null

  // Reset probability is encoded in an int. If a vertex value has bit T set,
  // it is included in the seed-set for topic-computation with number T.
  def resetProbability(v: VertexInfo[java.lang.Integer, java.lang.Float], computationId: Int, numVertices: Int) : Float = {
    if ((v.value() & (1 << computationId)) == 0) 0.0f else 1.0f  // Do not divide by numVertices, no need to normalize (?).
  }

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

  // Create the vertex-data file
  def initVertexData[T](graphname: String, graphchiSqr: GraphChiSquared[T]) {
    val vertexDataFile = new File(ChiFilenames.getFilenameOfVertexData(graphname, new IntConverter(), false))
    if (vertexDataFile.exists()) vertexDataFile.delete()

    val vertexVals = VertexData.createIntArray(graphchiSqr.numVertices())

    // Clean up....
    topicInfos.zip((0 until topicInfos.size)).foreach( x => { val (topicInfo, compidx) = x
      Source.fromFile(topicInfo.seedFile).getLines().filter(_.length > 0)foreach(line => {
        val vertexId = Integer.parseInt(line.split("\t")(0))
        if (vertexId < vertexVals.length) {   // Graph and the topic-seeds might be out of sync
          vertexVals(vertexId) |= (1 << compidx)
          graphchiSqr.initValue(compidx, vertexId, RESETPROB)
        } else println("Warning: too large vertex-id in topic top:", vertexId)
      })
    })


    // Create the vertexData
    val dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(vertexDataFile)))
    vertexVals.foreach(v => dos.writeInt(v))

    dos.close()
  }



  def main(args: Array[String]): Unit = {
    val graphname = args(0)
    val nshards = Integer.parseInt(args(1))
    val niters = Integer.parseInt(args(2))
    val initfile = args(3)

    val nComputations = initialize(initfile)
    val graphchiSqr = new GraphChiSquared[java.lang.Float](graphname, nshards, nComputations)
    val numVertices = graphchiSqr.numVertices()


    ChiFilenames.vertexDataSuffix = ".seeded." + ChiFilenames.getPid()
    println("Initializing... Starting ", nComputations, " computations")
    initVertexData(graphname, graphchiSqr)
    println("Done initializing")


    /* Compute */
    graphchiSqr.compute(niters,
      gatherInit = 0.0f,
      gather =  (v, vertexId, neighborVal, gather, compid) => gather + neighborVal,
      apply = (v, gather, compid) => (RESETPROB * resetProbability(v, compid, numVertices) +
          (1-RESETPROB) * gather) / v.numOutEdges(),
      vertexFilter = (v => v.numOutEdges() > 0)
    )


    println("Scale by outdegree")
    val degreeData = new DegreeData(graphname)
    var i = 0
    val chunk = 1000000
    while (i < numVertices) {
       val last = scala.math.min(i + chunk - 1, numVertices - 1)
       degreeData.load(i, last)
       var j = i

       while (j < last) {
         val outdeg = degreeData.getDegree(j).outDegree
         graphchiSqr.getVertexMatrix().multiplyRow(j, outdeg)
         j += 1
      }
       i += chunk
    }


      println("Ready, writing toplists...")

    /* Output top-lists */
    val ntop = 10000;
    (0 until nComputations).foreach(icomp => {
      val topList = Toplist.topList(graphchiSqr.getVertexMatrix(), icomp, ntop)
      val outputfile = "toplist." + topicInfos(icomp).topicName + ".tsv"

      val writer = new BufferedWriter(new FileWriter(new File(outputfile)));
      topList.foreach( item => writer.write(item.getVertexId + "\t" + topicInfos(icomp).topicName + "\t" + item.getValue() +"\n"))

      writer.close()
    })

    // Delete vertex data file
    new File(ChiFilenames.getFilenameOfVertexData(graphname, new IntConverter(), false)).delete()
  }
}