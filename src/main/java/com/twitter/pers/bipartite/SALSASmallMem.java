package com.twitter.pers.bipartite;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.vertexdata.ForeachCallback;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.FloatPair;
import edu.cmu.graphchi.datablocks.FloatPairConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.hadoop.PigGraphChiBase;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexProcessor;
import edu.cmu.graphchi.util.IdFloat;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * Version of SALSA that uses just a little memory (values propagated
 * via edges), and can be run under Pig.
 *
 * On each iteration either left or right side is computed. Each vertex
 * can represent both sides. Left side has out-edges, right side in-edges.
 * Left side = authorities (users)
 * Right side = hubs
 *
 * The algorithm starts with the right side, and the edges have initial
 * values for the left side vertices (authorities).
 *
 * @author Aapo Kyrola, akyrola@cs.cmu.edu
 * @copyright Twitter  (done during internship, Fall 2012)
 */
public class SALSASmallMem extends PigGraphChiBase implements GraphChiProgram<FloatPair, Float>  {


    private final static int RIGHTSIDE = 0; // Start with right side
    private final static int LEFTSIDE = 1;

    private String graphName;
    private final static Logger logger = ChiLogger.getLogger("salsa-smallmem");

    int numShards = 20;

    GraphChiEngine<FloatPair, Float> engine;

    public SALSASmallMem() {
        super();
    }

    @Override
    public void update(ChiVertex<FloatPair, Float> vertex, GraphChiContext context) {
        int side = context.getIteration() % 2;
        if (vertex.numEdges() > 0) {
            float nbrSum = 0.0f;

            if (side == LEFTSIDE) {
                for(int i=0; i < vertex.numOutEdges(); i++) {
                    nbrSum += vertex.outEdge(i).getValue();
                }
            } else {
                for(int i=0; i < vertex.numInEdges(); i++) {
                    nbrSum += vertex.inEdge(i).getValue();
                }
            }

            float newValue = nbrSum;

            FloatPair curValue = vertex.getValue();
            if (side == LEFTSIDE && vertex.numOutEdges() > 0) {
                curValue = new FloatPair(newValue, curValue.second);
                // Write value to outedges
                float broadcastValue = newValue / vertex.numOutEdges();
                for(int i=0; i < vertex.numOutEdges(); i++) {
                    vertex.outEdge(i).setValue(broadcastValue);
                }
            }
            else if (side == RIGHTSIDE && vertex.numInEdges() > 0) {
                // Renormalization
                int numRelevantEdges = vertex.numInEdges();
                int totalEdges = (int)  curValue.second;
                if (totalEdges == 0) {
                    logger.warning("Normalization factor cannot be zero! Id:" + context.getVertexIdTranslate().backward(vertex.getId()));
                    totalEdges = numRelevantEdges;
                }
                newValue *= numRelevantEdges * 1.0f / (float)totalEdges;

                // Write value to in-edges
                float broadcastValue = newValue / vertex.numInEdges();
                for(int i=0; i < vertex.numInEdges(); i++) {
                    vertex.inEdge(i).setValue(broadcastValue);
                }
            }
            vertex.setValue(curValue);
        }
    }

    @Override
    public void beginIteration(GraphChiContext ctx) {
    }


    @Override
    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    @Override
    public void endInterval(GraphChiContext ctx, VertexInterval interval) {
    }



    public void endIteration(GraphChiContext ctx) {

    }

    @Override
    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    @Override
    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void run(String graphName, int numShards) throws Exception {
        this.graphName = graphName;
        engine = new GraphChiEngine<FloatPair, Float>(graphName, numShards);
        engine.setEnableScheduler(false);
        engine.setSkipZeroDegreeVertices(true);
        engine.setEdataConverter(new FloatConverter());
        engine.setVertexDataConverter(new FloatPairConverter());
        engine.setMaxWindow(20000000);
        engine.run(this, 8);
    }

    private void outputResults(String graphName) throws IOException {

        VertexAggregator.foreach(engine.numVertices(), graphName, new FloatPairConverter(), new ForeachCallback<FloatPair>() {
            @Override
            public void callback(int vertexId, FloatPair vertexValue) {
                if (vertexValue.first > 0) {
                    System.out.println(engine.getVertexIdTranslate().backward(vertexId)  + "\t" + vertexValue.first);
                }
            }
        });
    }

    /**
     ]     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws  Exception {
        int k = 0;
        String graphName = null;
        if (args.length == 2) graphName = args[k++];
        int nShards = Integer.parseInt(args[k++]);
        SALSASmallMem hits = new SALSASmallMem();

        if (graphName == null) {
            graphName = "pipein";
            FastSharder sharder = hits.createSharder(graphName, nShards);
            sharder.shard(System.in);
        }
        hits.run(graphName, nShards);

        hits.outputResults(graphName);
    }

    // PIG support


    @Override
    protected String getSchemaString() {
        return "(weight:float, vertex:int)";
    }

    @Override
    protected int getNumShards() {
        return numShards;
    }

    private ArrayList<IdFloat> results;
    private Iterator<IdFloat> resultIter;

    @Override
    protected void runGraphChi() throws Exception {
        run(getGraphName(), getNumShards());
        results = new ArrayList<IdFloat>(100000);

        // Collect results - into memory ... This may consume a lot of memory.
        // It would be better to have an iterator for the vertex data.
        VertexAggregator.foreach(engine.numVertices(), graphName, new FloatPairConverter(), new ForeachCallback<FloatPair>() {
            @Override
            public void callback(int vertexId, FloatPair vertexValue) {
                if (vertexValue.first > 0) {
                    results.add(new IdFloat(engine.getVertexIdTranslate().backward(vertexId), vertexValue.first));
                }
            }
        });
        engine = null;
        resultIter = results.iterator();
    }

    @Override
    protected FastSharder createSharder(String graphName, int numShards) throws IOException {
        this.numShards = numShards;
        return new FastSharder<FloatPair, Float>(graphName, numShards, new VertexProcessor<FloatPair>() {
            @Override
            /* For lists (hubs), the vertex value will encode the total number of edges */
            public FloatPair receiveVertexValue(int vertexId, String token) {
                return new FloatPair(0.0f, Float.parseFloat(token));
            }
        }, new EdgeProcessor<Float>() {
            @Override
            public Float receiveEdge(int from, int to, String token) {
                return Float.parseFloat(token);
            }
        }, new FloatPairConverter(), new FloatConverter());
    }



    @Override
    protected Tuple getNextResult(TupleFactory tupleFactory) throws ExecException {
        if (resultIter.hasNext()) {
            IdFloat res = resultIter.next();
            Tuple t = tupleFactory.newTuple(2);
            t.set(0, res.getValue());
            t.set(1, res.getVertexId());
            return t;
        } else {
            return null;
        }
    }

    @Override
    protected String getStatusString() {
        if (engine != null) {
            GraphChiContext ctx = engine.getContext();
            if (ctx != null) {
                return ctx.getCurInterval() + " iteration: " +  ctx.getIteration() + "/" + ctx.getNumIterations();
            }
        }
        return "Initializing";
    }
}

