package edu.cmu.graphchi.scala

import edu.cmu.graphchi._
import edu.cmu.graphchi.engine._
import edu.cmu.graphchi.datablocks._

class ScalaChiVertex[VertexDataType, EdgeDataType](v : ChiVertex[VertexDataType, EdgeDataType]) 
{

	def inEdges() = {
		(0 until v.numInEdges()).map( i => v.inEdge(i))
	}

	def inEdgeValues() = {
		(0 until v.numInEdges()).map( i => v.inEdge(i).getValue())
	}

	def outEdges() = {
		(0 until v.numOutEdges()).map( i => v.outEdge(i))
	}


	def outEdgeValues() = {
		(0 until v.numOutEdges()).map( i => v.outEdge(i).getValue())
	}

	def edges() = {
		(0 until v.numEdges()).map( i => v.edge(i))
	}

	def edgeValues() = {
		(0 until v.numEdges()).map( i => v.edge(i).getValue())
	}

	def id() = {
		v.getId()
	}

	def value() = {
		v.getValue() 
	}

	def scatterOutedges(scatterFunc : ChiEdge[EdgeDataType] => EdgeDataType) = {
		outEdges().foreach( e => e.setValue(scatterFunc(e)))
	}

	def scatterInedges(scatterFunc : ChiEdge[EdgeDataType] => EdgeDataType) = {
		inEdges().foreach( e => e.setValue(scatterFunc(e)))
	}

	def scatterEdges(scatterFunc : ChiEdge[EdgeDataType] => EdgeDataType) = {
		edges().foreach( e => e.setValue(scatterFunc(e)))
	}

	def setValue(value : VertexDataType) = { v.setValue(value) }

	def outDegree = v.numOutEdges()

			def inDegree = v.numInEdges()

			def degree = v.numEdges()

}

abstract class EdgeDirection 
case class INEDGES() extends EdgeDirection
case class OUTEDGES() extends EdgeDirection
case class ALLEDGES()  extends EdgeDirection
case class NOEDGES() extends EdgeDirection

class GraphChiScala[VertexDataType, EdgeDataType, GatherType](baseFilename : String, numShards: Int)
extends GraphChiProgram[VertexDataType, EdgeDataType] {

	val engine = new GraphChiEngine[VertexDataType, EdgeDataType](baseFilename, numShards);

	def setEdataConverter(conv: BytesToValueConverter[EdgeDataType] ) : Unit = {
			engine.setEdataConverter(conv);
	}

	def setVertexDataConverter(conv : BytesToValueConverter[VertexDataType]) : Unit = {
			engine.setVertexDataConverter(conv) 
	}

  def numVertices = engine.numVertices()
  def vertexTranslate = engine.getVertexIdTranslate

	var initializer : Option[ScalaChiVertex[VertexDataType, EdgeDataType] => VertexDataType] = None

			def initializeVertices(initfunc : ScalaChiVertex[VertexDataType, EdgeDataType] => VertexDataType) = {
				initializer = Some(initfunc)
			}

			var gatherFunc : (ScalaChiVertex[VertexDataType, EdgeDataType],  EdgeDataType, Int, GatherType) => GatherType = null
			var scatterFunc : (ScalaChiVertex[VertexDataType, EdgeDataType], EdgeDataType, Int) => EdgeDataType = null
			var constScatterFunc : ScalaChiVertex[VertexDataType, EdgeDataType] => EdgeDataType = null
			var applyFunc : (GatherType, ScalaChiVertex[VertexDataType, EdgeDataType]) => VertexDataType = null
			
			var scatterDir : EdgeDirection = NOEDGES()
			var gatherDir : EdgeDirection = NOEDGES()
			var gatherInitVal : Option[GatherType] = None
			
			def foreach(iters : Int, 
			        gatherDirection : EdgeDirection,
			        gatherInit : GatherType,
			        gather : (ScalaChiVertex[VertexDataType, EdgeDataType], EdgeDataType, Int, GatherType) => GatherType,
			        apply: (GatherType, ScalaChiVertex[VertexDataType, EdgeDataType]) => VertexDataType,
			        scatterDirection : EdgeDirection, 
					scatter : (ScalaChiVertex[VertexDataType, EdgeDataType], EdgeDataType, Int) => EdgeDataType) = {
			    gatherDir = gatherDirection
			    gatherInitVal = Some(gatherInit)
				gatherFunc = gather
				scatterDir = scatterDirection
				scatterFunc = scatter
				
				scatterDir match {
					  case OUTEDGES() => engine.setModifiesInedges(false)
					  case ALLEDGES() => {}
					  case INEDGES() =>  engine.setModifiesOutedges(false)
					  case NOEDGES() => { engine.setModifiesOutedges(false); engine.setModifiesInedges(false) }
					}
				engine.run(this, iters);
			}
			
			def foreach(iters : Int, 
			        gatherDirection : EdgeDirection,
			        gatherInit : GatherType,
			        gather : (ScalaChiVertex[VertexDataType, EdgeDataType], EdgeDataType, Int, GatherType) => GatherType,
			        apply: (GatherType, ScalaChiVertex[VertexDataType, EdgeDataType]) => VertexDataType,
			        scatterDirection : EdgeDirection, 
					scatter : ScalaChiVertex[VertexDataType, EdgeDataType] => EdgeDataType) = {
				gatherDir = gatherDirection
				gatherInitVal = Some(gatherInit)
				gatherFunc = gather
				applyFunc = apply
				scatterDir = scatterDirection
				constScatterFunc = scatter
				
				scatterDir match {
					  case OUTEDGES() => engine.setModifiesInedges(false)
					  case ALLEDGES() => {}
					  case INEDGES() =>  engine.setModifiesOutedges(false)
					  case NOEDGES() => { engine.setModifiesOutedges(false); engine.setModifiesInedges(false) }
					}
				
				engine.run(this, iters);
			}

			override def update(v : ChiVertex[VertexDataType, EdgeDataType], ctx : GraphChiContext) : Unit = {
				var skipGather : Boolean = false; 
				if (ctx.getIteration() == 0) {
					initializer match {
						case None => {}
						case Some(initfunc) => { v.setValue(initfunc(new ScalaChiVertex[VertexDataType, EdgeDataType](v)))
								skipGather = true}
					}
				}
				// How to model the flow better?
				val sv = new ScalaChiVertex[VertexDataType, EdgeDataType](v)
				if (!skipGather) {
					var gatherVal = gatherInitVal match {
					  case None => throw new RuntimeException("Gather needs an init value")
					  case Some(gval) => gval
					}
					gatherDir match {
					  // NOTE: this is not parallel....
					  case OUTEDGES() => sv.outEdges().foreach(e => gatherVal = gatherFunc(sv, e.getValue(), ctx.getIteration(), gatherVal))
					  case ALLEDGES() => sv.edges().foreach(e => gatherVal = gatherFunc(sv, e.getValue(), ctx.getIteration(), gatherVal))
					  case INEDGES() => sv.inEdges().foreach(e => gatherVal = gatherFunc(sv, e.getValue(), ctx.getIteration(), gatherVal))
					  case NOEDGES() => sv.outEdges().foreach(e => gatherVal = gatherFunc(sv, e.getValue(), ctx.getIteration(), gatherVal))
					} 
					v.setValue(applyFunc(gatherVal, sv))
				}
				
				if (scatterFunc != null) {
					scatterDir match {
					  case OUTEDGES() => sv.outEdges().foreach( e => e.setValue(scatterFunc(sv, e.getValue(), ctx.getIteration())))
					  case ALLEDGES() => sv.edges().foreach( e => e.setValue(scatterFunc(sv, e.getValue(), ctx.getIteration())))
					  case INEDGES() => sv.inEdges().foreach( e => e.setValue(scatterFunc(sv, e.getValue(), ctx.getIteration())))
					  case NOEDGES() => {}
					}
				} else if (constScatterFunc != null) {
					  val ev : EdgeDataType = constScatterFunc(sv)
					  scatterDir match {
						  case OUTEDGES() => sv.outEdges().foreach( e => e.setValue(ev))
						  case ALLEDGES() => sv.edges().foreach( e => e.setValue(ev))
						  case INEDGES() => sv.inEdges().foreach( e => e.setValue(ev))
						  case NOEDGES() => {}
						}
				}
			}

			override def beginIteration(ctx : GraphChiContext) : Unit = {}
			override def endIteration(ctx : GraphChiContext) : Unit = {}
			override def beginInterval(ctx : GraphChiContext, interval : VertexInterval) : Unit = {}
			override def endInterval(ctx : GraphChiContext, interval : VertexInterval) : Unit = {}

      override def beginSubInterval(ctx: GraphChiContext, interval: VertexInterval) {}
      override def endSubInterval(ctx: GraphChiContext, interval: VertexInterval) {}
}

/*
    public void update(ChiVertex<VertexDataType, EdgeDataType> vertex, GraphChiContext context);

    public void beginIteration(GraphChiContext ctx);

    public void endIteration(GraphChiContext ctx);

    public void beginInterval(GraphChiContext ctx, VertexInterval interval);

    public void endInterval(GraphChiContext ctx, VertexInterval interval);*/