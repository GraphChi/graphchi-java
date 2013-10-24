package edu.cmu.graphchi.scala


import edu.cmu.graphchi._
import edu.cmu.graphchi.engine._
import edu.cmu.graphchi.datablocks._
import edu.cmu.graphchi.util._
import java.util.concurrent.TimeUnit

class VertexInfo[VT, ET](v : ChiVertex[VT, ET]) {
  def numInEdges() = v.numInEdges()
  def numOutEdges() = v.numOutEdges()
  def numEdges() = v.numEdges()
  def id() = v.getId()
  def value() = v.getValue()
}

/**
 * GraphChiSquared computes many (personalized) computations in parallel. Vertex
 * values are stored in-memory.
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 * @param baseFilename
 * @param numShards
 * @param numComputations
 */
class GraphChiSquared[GatherType : ClassManifest](baseFilename : String, numShards : Int, numComputations : Int)
    extends GraphChiProgram[java.lang.Integer, java.lang.Float]{
  type VertexDataType = java.lang.Integer
  type EdgeDataType = java.lang.Float
  type ComputationValueType = java.lang.Float
  val engine = new GraphChiEngine[VertexDataType, EdgeDataType](baseFilename, numShards);
  engine.setEdataConverter(new FloatConverter())
  engine.setVertexDataConverter(new IntConverter())
  //engine.setMemoryBudgetMb(1500)
  engine.setModifiesInedges(false)
  engine.setModifiesOutedges(false)
  engine.setEnableDeterministicExecution(false);


  val vertexMatrix = new HugeFloatMatrix(engine.numVertices(), numComputations)

  type GatherFunctionType = (VertexInfo[VertexDataType, EdgeDataType], Int, EdgeDataType, EdgeDataType, GatherType) => GatherType
  type OnlyAdjGatherFunctionType = (VertexInfo[VertexDataType, EdgeDataType], Int, EdgeDataType, GatherType, Int) => GatherType

  // Apply: (vertex-info, gather-value, computation-id)
  type ApplyFunctionType =  (VertexInfo[VertexDataType, EdgeDataType], GatherType, Int) => ComputationValueType

  var gatherFunc : GatherFunctionType  = null
  var gatherFuncOnlyAdjOutEdges : OnlyAdjGatherFunctionType  = null
  var gatherFuncOnlyAdj : OnlyAdjGatherFunctionType  = null
  var applyFunc : ApplyFunctionType = null
  var gatherInitVal : Option[GatherType] = None
  var filterFunc : (VertexInfo[VertexDataType, EdgeDataType] => Boolean) = (v => true);

  def initValues(computationId: Int, initValues: Iterable[(Int, java.lang.Float)]) = {
    initValues.foreach{ case (vertex, value) => vertexMatrix.setValue(vertex, computationId, value)}
  }

  def initValue(computationId: Int, vertexId: Int, value: java.lang.Float) =
    vertexMatrix.setValue(vertexId, computationId, value)

  def numVertices() = engine.numVertices()

  def compute(iterations: Int, gatherInit: GatherType, gather: GatherFunctionType, apply: ApplyFunctionType) {
    gatherFunc = gather
    applyFunc = apply
    gatherInitVal = Some(gatherInit)
    println ("Starting to run with " + numComputations + " parallel computations")
    engine.run(this, iterations)
  }

  def compute(iterations: Int, gatherInit: GatherType, gather: OnlyAdjGatherFunctionType, apply: ApplyFunctionType,
              vertexFilter : (VertexInfo[VertexDataType, EdgeDataType] => Boolean)) {
    gatherFuncOnlyAdj = gather
    applyFunc = apply
    gatherInitVal = Some(gatherInit)
    filterFunc = vertexFilter

    println ("Starting to run with " + numComputations + " parallel computations")

    engine.setEdataConverter(null)
    engine.setOnlyAdjacency(true)
    engine.setDisableOutEdges(true)   // No scatter
    engine.setAutoLoadNext(true)
    engine.run(this, iterations)
  }

  def compute(iterations: Int, gatherInit: GatherType, gather: OnlyAdjGatherFunctionType, gatherOut: OnlyAdjGatherFunctionType, apply: ApplyFunctionType,
              vertexFilter : (VertexInfo[VertexDataType, EdgeDataType] => Boolean)) {
    gatherFuncOnlyAdj = gather
    gatherFuncOnlyAdjOutEdges = gatherOut
    applyFunc = apply
    gatherInitVal = Some(gatherInit)
    filterFunc = vertexFilter

    println ("Starting to run with " + numComputations + " parallel computations")

    engine.setEdataConverter(null)
    engine.setOnlyAdjacency(true)
    engine.setAutoLoadNext(true)
    engine.run(this, iterations)
  }

  def getVertexValue(computationId: Int, vertexId: Int) = vertexMatrix.getValue(vertexId, computationId)

  override def update(v: ChiVertex[VertexDataType, EdgeDataType], ctx: GraphChiContext) : Unit = {

    val vertexInfo = new VertexInfo(v)
    if (filterFunc(vertexInfo)) {

      /********* IN-EDGES *********/
      /* Compute gathers */
      val n = v.numInEdges()
      var i = 0

      var gathers = new Array[GatherType](numComputations)
      var c = 0
      while ( c < numComputations) { gathers(c) = gatherInitVal.get; c += 1}

      while (i < n)  {  // Unfortunately higher order calls like "(0 until n)" are quite a bit slower
      val e = v.inEdge(i)
        var c = 0
        val nbid = e.getVertexId
        val rowblock = vertexMatrix.getRowBlock(nbid) // premature optimization!
        val blockIdx = vertexMatrix.getBlockIdx(nbid)
        while (c < numComputations) {
          if (gatherFunc != null) {
            gathers(c) = gatherFunc(vertexInfo, nbid, rowblock(blockIdx + c), e.getValue, gathers(c))
          } else if (gatherFuncOnlyAdj != null) {
            gathers(c) = gatherFuncOnlyAdj(vertexInfo, nbid, rowblock(blockIdx + c), gathers(c), c)
          }
          c += 1
        }

        i += 1
      }

      /******** OUT-EDGES - HACKY  *********/
      if (gatherFuncOnlyAdjOutEdges != null) {
        val on = v.numOutEdges()
        i = 0
        while (i < on)  {  // Unfortunately higher order calls like "(0 until n)" are quite a bit slower
        val e = v.outEdge(i)
          var c = 0
          val nbid = e.getVertexId
          val rowblock = vertexMatrix.getRowBlock(nbid) // premature optimization!
          val blockIdx = vertexMatrix.getBlockIdx(nbid)
          while (c < numComputations) {
            if (gatherFuncOnlyAdjOutEdges != null) {
              gathers(c) = gatherFuncOnlyAdjOutEdges(vertexInfo, nbid, rowblock(blockIdx + c), gathers(c), c)
            }
            c += 1
          }

          i += 1
        }
      }

      /* Apply and write into the matrix */
      c = 0
      while ( c < numComputations) {
        val newVertexVal = applyFunc(vertexInfo, gathers(c), c)
        vertexMatrix.setValue(v.getId, c, newVertexVal)
        c += 1
      }
    }
  }

  def getVertexMatrix() = vertexMatrix

  override def beginIteration(ctx : GraphChiContext) : Unit = {}
  override def endIteration(ctx : GraphChiContext) : Unit = {}
  override def beginInterval(ctx : GraphChiContext, interval : VertexInterval) : Unit = {}
  override def endInterval(ctx : GraphChiContext, interval : VertexInterval) : Unit = {}

  override def beginSubInterval(ctx: GraphChiContext, interval: VertexInterval) {}
  override def endSubInterval(ctx: GraphChiContext, interval: VertexInterval) {}
}