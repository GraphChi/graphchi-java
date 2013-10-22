package edu.cmu.graphchi.apps;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexProcessor;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import edu.cmu.graphchi.vertexdata.VertexIdValue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * Tests that the system works. Processes any graph and runs a
 * deterministic algorithm on the graph and checks that the
 * result is intended.
 * @author akyrola
 */
public class SmokeTest implements GraphChiProgram<Integer, Integer> {

    private static Logger logger = ChiLogger.getLogger("smoketest");

    // Keep track of loaded vertex values to check vertex value loading works
    private static HashMap<Integer, Integer> loadedVertexValues = new HashMap<Integer, Integer>();

    public void update(ChiVertex<Integer, Integer> vertex, GraphChiContext context) {
        if (context.getIteration() == 0) {
            /* Check vertex value was loaded properly */
            int originalId = context.getVertexIdTranslate().backward(vertex.getId());
            if (loadedVertexValues.containsKey(originalId)) {
                if (!vertex.getValue().equals(loadedVertexValues.get(originalId))) {
                    throw new RuntimeException("Vertex value for vertex " + originalId + " not loaded properly:" +
                            vertex.getValue() + " !=" + loadedVertexValues.get(originalId));
                } else {
                    logger.info("Loaded vertex value correctly: " + originalId + " = " + loadedVertexValues.get(originalId));
                }
            }

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
                if (token == null) {
                    return 0;
                } else {
                    synchronized (this) {
                       loadedVertexValues.put(vertexId, Integer.parseInt(token));
                    }
                    return Integer.parseInt(token);
                }
            }
        }, new EdgeProcessor<Integer>() {
            public Integer receiveEdge(int from, int to, String token) {
                return (token == null ? 0 : Integer.parseInt(token));
            }
        }, new IntConverter(), new IntConverter());
    }

    public static void main(String[] args) throws  Exception {
        String baseFilename = args[0];
        int nShards = Integer.parseInt(args[1]);
        String fileType = (args.length >= 3 ? args[2] : null);

        /* Create shards */
        FastSharder sharder = createSharder(baseFilename, nShards);
        sharder.setAllowSparseDegreesAndVertexData(false);
        if (baseFilename.equals("pipein")) {     // Allow piping graph in
            sharder.shard(System.in);
        } else {
            sharder.shard(new FileInputStream(new File(baseFilename)), fileType);
        }

        /* Run engine */
        GraphChiEngine<Integer, Integer> engine = new GraphChiEngine<Integer, Integer>(baseFilename, nShards);
        engine.setEdataConverter(new IntConverter());
        engine.setVertexDataConverter(new IntConverter());
        engine.setModifiesInedges(false); // Important optimization
        engine.run(new SmokeTest(), 5);

        /* Test vertex iterator */
        Iterator<VertexIdValue<Integer>> iter = VertexAggregator.vertexIterator(engine.numVertices(),
                baseFilename, new IntConverter(), engine.getVertexIdTranslate());

        int i=0;
        while(iter.hasNext()) {
            VertexIdValue<Integer> x = iter.next();
            int internalId = engine.getVertexIdTranslate().forward(x.getVertexId());
            int expectedValue = internalId + 4;
            if (expectedValue != x.getValue()) {
                throw new IllegalStateException("Expected internal value to be " + expectedValue
                        + ", but it was " + x.getValue() + "; internal id=" + internalId + "; orig=" + x.getVertexId());
            }
            if (i % 10000 == 0) {
                logger.info("Scanning vertices: " + i);
            }
            i++;
        }

        if (i != engine.numVertices())
            throw new IllegalStateException("Error in iterator: did not have numVertices vertices: " + i + "/" + engine.numVertices());

        logger.info("Ready.");
    }
}
