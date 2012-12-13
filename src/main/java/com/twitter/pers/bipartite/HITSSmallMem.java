package com.twitter.pers.bipartite;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.LoggingInitializer;
import edu.cmu.graphchi.aggregators.ForeachCallback;
import edu.cmu.graphchi.aggregators.VertexAggregator;
import edu.cmu.graphchi.aggregators.VertexMapper;
import edu.cmu.graphchi.aggregators.VertexMapperCallback;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.FloatPair;
import edu.cmu.graphchi.datablocks.FloatPairConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.util.IdFloat;
import edu.cmu.graphchi.util.Toplist;

import java.io.IOException;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * Version of HITS that uses just a little memory (values propagated
 * via edges), and can be run under Pig.
 *
 * On each iteration either left or right side is computed. Each vertex
 * can represent both sides. Left side has out-edges, right side in-edges.
 */
public class HITSSmallMem implements GraphChiProgram<FloatPair, Float> {

    private final static int LEFTSIDE = 0;
    private final static int RIGHTSIDE = 1;

    private String graphName;
    private final static Logger logger = LoggingInitializer.getLogger("hits-smallmem");

    double leftSideSqrSum = 0;
    double rightSideSqrSum = 0;

    @Override
    public void update(ChiVertex<FloatPair, Float> vertex, GraphChiContext context) {
        int side = context.getIteration() % 2;
        if (vertex.numEdges() > 0) {
            float nbrSum = 0.0f;

            if ((side == LEFTSIDE) && context.getIteration() == 0) {
                nbrSum = vertex.numOutEdges() * 1.0f;
            } else {
                if (side == LEFTSIDE) {
                    for(int i=0; i < vertex.numOutEdges(); i++) {
                        nbrSum += vertex.outEdge(i).getValue();
                    }
                } else {
                    for(int i=0; i < vertex.numInEdges(); i++) {
                        nbrSum += vertex.inEdge(i).getValue();
                    }
                }
            }

            float newValue = nbrSum;

            FloatPair curValue = vertex.getValue();
            if (side == LEFTSIDE && vertex.numOutEdges() > 0) {
                curValue.first = newValue;
                synchronized (this) {
                    leftSideSqrSum += newValue * newValue;
                }

                // Write value to outedges
                for(int i=0; i < vertex.numOutEdges(); i++) {
                     vertex.outEdge(i).setValue(newValue);
                }
            }
            else if (side == RIGHTSIDE && vertex.numInEdges() > 0) {
                curValue.second = newValue;
                synchronized (this) {
                    rightSideSqrSum += newValue * newValue;
                }

                // Write value to in-edges
                for(int i=0; i < vertex.numInEdges(); i++) {
                    vertex.inEdge(i).setValue(newValue);
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
        if (ctx.getIteration() % 2 == 1) {
            try {
                logger.info("NORMALIZING");
                // Normalize both sides by their square...
                final float leftNorm = (float) Math.sqrt(leftSideSqrSum);
                final float rightNorm = (float) Math.sqrt(rightSideSqrSum);

                logger.info("Left side norm: " + leftNorm + ", right: " + rightNorm);

                VertexMapper.map((int) ctx.getNumVertices(), graphName, new FloatPairConverter(), new VertexMapperCallback<FloatPair>() {
                    @Override
                    public FloatPair map(int vertexId, FloatPair value) {
                        value.first /= leftNorm;
                        value.second /= rightNorm;
                        return value;
                    }
                });

                leftSideSqrSum = 0.0;
                rightSideSqrSum = 0.0;
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    @Override
    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    @Override
    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void run(String graphName, int numShards) throws Exception {
        this.graphName = graphName;
        GraphChiEngine<FloatPair, Float> engine = new GraphChiEngine<FloatPair, Float>(graphName, numShards);
        engine.setEnableScheduler(false);
        engine.setSkipZeroDegreeVertices(true);
        engine.setEdataConverter(new FloatConverter());
        engine.setVertexDataConverter(new FloatPairConverter());
        engine.setMaxWindow(20000000);
        engine.run(this, 8);

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

        if (graphName == null) {
            graphName = "pipein";
            FastSharder sharder = new FastSharder<Float>(graphName, nShards, new EdgeProcessor<Float>() {
                @Override
                public void receiveVertexValue(int vertexId, String token) {
                }

                @Override
                public Float receiveEdge(int from, int to, String token) {
                    return 0.0f;
                }
            }, new FloatConverter());
            sharder.shard(System.in);
        }
        HITSSmallMem hits = new HITSSmallMem();
        hits.run(graphName, nShards);
    }
}

