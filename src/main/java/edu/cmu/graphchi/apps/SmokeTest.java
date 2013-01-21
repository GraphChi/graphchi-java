package edu.cmu.graphchi.apps;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexProcessor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * Tests that the system works.
 * @author akyrola
 *         Date: 7/11/12
 */
public class SmokeTest implements GraphChiProgram<Integer, Integer> {

    private static Logger logger = LoggingInitializer.getLogger("smoketest");


    public void update(ChiVertex<Integer, Integer> vertex, GraphChiContext context) {
        if (context.getIteration() == 0) {
            vertex.setValue(vertex.getId() + context.getIteration());
        } else {
            int curval = vertex.getValue();
            int vexpected = vertex.getId() + context.getIteration() - 1;
            if (curval != vexpected) {
                throw new RuntimeException("Mismatch (vertex). Expected: " + vexpected + " but had " +
                        curval);

            }
            for(int i=0; i<vertex.numInEdges(); i++) {
                int has = vertex.inEdge(i).getValue();
                int correction = vertex.getId() > vertex.inEdge(i).getVertexId() ? +1 : 0;
                int expected = vertex.inEdge(i).getVertexId() + context.getIteration() - 1 + correction;
                if (expected != has)
                    throw new RuntimeException("Mismatch (edge): " + expected + " expected but had "+ has +
                            ". Iteration:" + context.getIteration() + ", edge:" + vertex.inEdge(i).getVertexId()
                            + " -> " + vertex.getId());
            }
            vertex.setValue(vertex.getId() + context.getIteration());

        }
        int val = vertex.getValue();
        for(int i=0; i<vertex.numOutEdges(); i++) {
            vertex.outEdge(i).setValue(val);
        }
    }


    public void beginIteration(GraphChiContext ctx) {}

    public void endIteration(GraphChiContext ctx) {}

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}


    @Override
    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    @Override
    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    /**
     * Initialize the sharder-program.
     * @param graphName
     * @param numShards
     * @return
     * @throws java.io.IOException
     */
    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Integer, Integer>(graphName, numShards, new VertexProcessor<Integer>() {
            public Integer receiveVertexValue(int vertexId, String token) {
                return 0;
            }
        }, new EdgeProcessor<Integer>() {
            public Integer receiveEdge(int from, int to, String token) {
                return 0;
            }
        }, new IntConverter(), new IntConverter());
    }

    public static void main(String[] args) throws  Exception {
        String baseFilename = args[0];
        int nShards = Integer.parseInt(args[1]);

        /* Create shards */
        FastSharder sharder = createSharder(baseFilename, nShards);
        if (baseFilename.equals("pipein")) {     // Allow piping graph in
            sharder.shard(System.in);
        } else {
            sharder.shard(new FileInputStream(new File(baseFilename)));
        }

        /* Run engine */
        GraphChiEngine<Integer, Integer> engine = new GraphChiEngine<Integer, Integer>(baseFilename, nShards);
        engine.setEdataConverter(new IntConverter());
        engine.setVertexDataConverter(new IntConverter());
        engine.setModifiesInedges(false); // Important optimization
        engine.run(new SmokeTest(), 5);

        logger.info("Ready.");
    }
}
