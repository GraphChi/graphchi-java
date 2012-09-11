package edu.cmu.graphchi.scala.apps

import edu.cmu.graphchi.scala._
import edu.cmu.graphchi.datablocks.FloatConverter
import edu.cmu.graphchi.util.IdFloat
import edu.cmu.graphchi.util.Toplist
import java.util.TreeSet
import scala.collection.JavaConversions._
import java.util.Random

object PersonalizedPagerank {
   
    def main(args: Array[String]): Unit = {
        val filename = args(0)
        val nshards = Integer.parseInt(args(1))
        val niters = Integer.parseInt(args(2))
        val nComputations = Integer.parseInt(args(3))
        
        val graphchiSqr = new GraphChiSquaredPrototype(filename, nshards, nComputations)
        
        println("Initializing...")
        var i = 0
        var n = graphchiSqr.numVertices()
        while (i < n) {  // (0 until n) is way too slow
        	graphchiSqr.initValues(0, List((i, 1.0f)))
        	i += 1
        }
        
        /* Randomly initialize others */
        val rand = new Random()
        (0 until nComputations).foreach( c => {
            val v = Math.abs(rand.nextInt()) % graphchiSqr.numVertices()
            println("Init: " + c + ", " + v )
        	graphchiSqr.initValues(c, List((v, 1.0f.asInstanceOf[java.lang.Float])))
        })
        println("Done initializing")
        
        
        graphchiSqr.compute(niters,
            gatherInit = 0.0f,
            gather =  (v, neighborVal, gather) => gather + neighborVal,
            apply = (v, gather) => (0.15f + 0.85f * gather) / v.numOutEdges()
        )
        
        /* Debug */
        (0 until nComputations).foreach(i => {
          if (n > 24000000)
            println("Vertex " + i + " / 24000000: " + graphchiSqr.getVertexValue(i, 24000000))
            println("Vertex " + i + " / 8737: " + graphchiSqr.getVertexValue(i, 8737))
        }
        )
    }
}