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
import edu.cmu.graphchi.engine.auxdata.VertexData

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
         return (v.value() & (1 << computationId)) * 1.0f;  // Do not divide by numVertices, no need to normalize (?).
    }

    def initialize(initfile: String) : Int = {
          topicInfos = Source.fromFile(initfile).getLines().filter(_.indexOf("\t") > 0).map( line  => {
              val toks = line.split("\t")
              TopicInfo(toks(0), toks(1))
          }).toArray
         println(topicInfos)
         topicInfos.size
    }

  // Create the vertex-data file
   def initVertexData(graphname: String, graphchiSqr: GraphChiSquared) {
       val vertexDataFile = new File(ChiFilenames.getFilenameOfVertexData(graphname, new IntConverter()))
       if (vertexDataFile.exists()) vertexDataFile.delete()

       val vertexVals = VertexData.createIntArray(graphchiSqr.numVertices())

       // Clean up....
       topicInfos.zip((0 until topicInfos.size)).foreach( x => { val (topicInfo, compidx) = x
            Source.fromFile(topicInfo.seedFile).getLines().filter(_.length > 0)foreach(line => {
                val vertexId = Integer.parseInt(line.split("\t")(0))
                vertexVals(vertexId) |= (1 << compidx)
                graphchiSqr.initValue(compidx, vertexId, RESETPROB)
            })
       })

      println("Debug: 287418 = %d", vertexVals(287418))

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
        val graphchiSqr = new GraphChiSquared(graphname, nshards, nComputations)
        val numVertices = graphchiSqr.numVertices()


        println("Initializing... Starting %f computations", nComputations)
        initVertexData(graphname, graphchiSqr)
        println("Done initializing")
        

        /* Compute */
        graphchiSqr.compute(niters,
            gatherInit = 0.0f,
            gather =  (v, vertexId, neighborVal, gather) => gather + neighborVal,
            apply = (v, gather, compid) => (RESETPROB * resetProbability(v, compid, numVertices) +
                            (1-RESETPROB) * gather) / v.numOutEdges()
        )

       println("Ready, writing toplists...")

       /* Output top-lists */
       val ntop = 1000;
       (0 until nComputations).foreach(icomp => {
           val topList = Toplist.topList(graphchiSqr.getVertexMatrix(), icomp, ntop)
           val outputfile = "toplist." + topicInfos(icomp).topicName + ".tsv"

           val writer = new BufferedWriter(new FileWriter(new File(outputfile)));
           topList.foreach( item => writer.write(item.getVertexId + "\t" + item.getValue() +"\n"))

           writer.close()
       })
    }
}