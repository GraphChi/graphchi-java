package edu.cmu.graphchi.scala


import edu.cmu.graphchi._
import edu.cmu.graphchi.engine._
import edu.cmu.graphchi.datablocks._
import edu.cmu.graphchi.util._

class VertexInfo[VT, ET](v : ChiVertex[VT, ET]) {
	def numInEdges() = v.numInEdges()
	def numOutEdges() = v.numOutEdges()
	def numEdges() = v.numEdges()
	def id() = v.getId()
}

class GraphChiSquaredPrototype(baseFilename : String, numShards : Int, numComputations : Int) 
 extends GraphChiProgram[java.lang.Float, java.lang.Float]{
	type VertexDataType = java.lang.Float;
	type EdgeDataType = java.lang.Float;
	type GatherType = java.lang.Float;
	val engine = new GraphChiEngine[VertexDataType, EdgeDataType](baseFilename, numShards);
	engine.setEdataConverter(new FloatConverter())
	engine.setVertexDataConverter(new FloatConverter())
	engine.setMemoryBudgetMb(1500)
	engine.setModifiesInedges(false)
	engine.setModifiesOutedges(false)
	engine.setEnableDeterministicExecution(false);
	
	val vertexMatrix = new HugeFloatMatrix(engine.numVertices(), numComputations)
	
	type GatherFunctionType = (VertexInfo[VertexDataType, EdgeDataType], Int, EdgeDataType, EdgeDataType, GatherType) => GatherType
  type OnlyAdjGatherFunctionType = (VertexInfo[VertexDataType, EdgeDataType], Int, EdgeDataType, GatherType) => GatherType

  type ApplyFunctionType =  (VertexInfo[VertexDataType, EdgeDataType], GatherType) => VertexDataType

	var gatherFunc : GatherFunctionType  = null;
  var gatherFuncOnlyAdj : OnlyAdjGatherFunctionType  = null;
	var applyFunc : ApplyFunctionType = null;
	var gatherInitVal : GatherType = 0.0f
 
	
	def initValues(computationId : Int, initValues : Iterable[(Int, java.lang.Float)]) = {
	    initValues.foreach{ case (vertex, value) => vertexMatrix.setValue(vertex, computationId, value)}
	}
	
	def numVertices() = engine.numVertices()
	
	def compute(iterations : Int, gatherInit : GatherType, gather: GatherFunctionType, apply : ApplyFunctionType) {
	    gatherFunc = gather;
	    applyFunc = apply;
	    gatherInitVal = gatherInit
	    println ("Starting to run with " + numComputations + " parallel computations")
	    engine.run(this, iterations)
	}

  def compute(iterations : Int, gatherInit : GatherType, gather: OnlyAdjGatherFunctionType, apply : ApplyFunctionType) {
    gatherFuncOnlyAdj = gather;
    applyFunc = apply;
    gatherInitVal = gatherInit
    println ("Starting to run with " + numComputations + " parallel computations")

    engine.setEdataConverter(null)
    engine.setOnlyAdjacency(true)
    engine.run(this, iterations)
  }
	
	def getVertexValue(computationId : Int, vertexId : Int) = vertexMatrix.getValue(vertexId, computationId)
	
	override def update(v : ChiVertex[VertexDataType, EdgeDataType], ctx : GraphChiContext) : Unit = {
		var gathers = new Array[GatherType](numComputations)
		var c = 0
		while ( c < numComputations) { gathers(c) = gatherInitVal; c += 1}
		val vertexInfo = new VertexInfo(v)
		
		/* Compute gathers */
		val n = v.numInEdges()
		var i = 0
		

		while (i < n)  {  // Unfortunately higher order calls like "(0 until n)" are quite a bit slower
		    val e = v.inEdge(i)
		    var c = 0
        val nbid = e.getVertexId()
		    val rowblock = vertexMatrix.getRowBlock(nbid) // premature optimization!
		    val blockIdx = vertexMatrix.getBlockIdx(nbid)
		    while (c < numComputations) {
            if (gatherFunc != null) {
		          gathers(c) = gatherFunc(vertexInfo, nbid, rowblock(blockIdx + c), e.getValue(), gathers(c))
            } else if (gatherFuncOnlyAdj != null) {
              gathers(c) = gatherFuncOnlyAdj(vertexInfo, nbid, rowblock(blockIdx + c), gathers(c))
            }
		        c += 1
		    }
		    
		    i += 1
		}
		
		/* Apply and write into the matrix */
		c = 0
		while ( c < numComputations) {
		   val newVertexVal = applyFunc(vertexInfo, gathers(c))
		   vertexMatrix.setValue(v.getId(), c, newVertexVal)
		   c += 1
		}
	}

	override def beginIteration(ctx : GraphChiContext) : Unit = {}
	override def endIteration(ctx : GraphChiContext) : Unit = {}
	override def beginInterval(ctx : GraphChiContext, interval : VertexInterval) : Unit = {}
	override def endInterval(ctx : GraphChiContext, interval : VertexInterval) : Unit = {}
}