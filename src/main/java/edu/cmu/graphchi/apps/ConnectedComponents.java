package edu.cmu.graphchi.apps;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexProcessor;
import edu.cmu.graphchi.util.LabelAnalysis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Example application for computing the weakly connected components
 * of a graph. The algorithm uses label exchange: each vertex first chooses
 * a label equaling its id; on the subsequent iterations each vertex sets
 * its label to be the minimum of the neighbors' labels and its current label.
 * Algorithm finishes when no labels change. Each vertex with same label belongs
 * to same component.
 * @author akyrola
 */
public class ConnectedComponents implements GraphChiProgram<Integer, Integer> {

    private static Logger logger = ChiLogger.getLogger("connectedcomponents");

    public void update(ChiVertex<Integer, Integer> vertex, GraphChiContext context) {
        final int iteration = context.getIteration();
        final int numEdges = vertex.numEdges();

        /* On first iteration, each vertex chooses a label equalling its id */
        if (iteration == 0) {
            vertex.setValue(vertex.getId());

            /* Schedule the vertex itself for execution on next iteration */
            context.getScheduler().addTask(vertex.getId());
        }


        /* Choose the smallest id of neighbor vertices. Each vertex
          writes its label to its edges, so it can be accessed by neighbors.
         */
        int curMin = vertex.getValue();
        for(int i=0; i < numEdges; i++) {
            int nbLabel = vertex.edge(i).getValue();
            if (iteration == 0) nbLabel = vertex.edge(i).getVertexId(); // Note!
            if (nbLabel < curMin) {
                curMin = nbLabel;
            }
        }

        /**
         * Set my new label
         */
        vertex.setValue(curMin);
        int label = curMin;

        /**
         * Broadcast my value to neighbors by writing the value to my edges.
         */
        if (iteration > 0) {
            for(int i=0; i < numEdges; i++) {
                if (vertex.edge(i).getValue() > label) {
                    vertex.edge(i).setValue(label);
                    context.getScheduler().addTask(vertex.edge(i).getVertexId());
                }
            }
        } else {
            // Special case for first iteration to avoid overwriting
            for(int i=0; i < vertex.numOutEdges(); i++) {
                vertex.outEdge(i).setValue(label);
            }
        }
    }


    public void beginIteration(GraphChiContext ctx) {}

    public void endIteration(GraphChiContext ctx) {}

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {}

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


    /**
     * Usage: java edu.cmu.graphchi.demo.ConnectedComponents graph-name num-shards filetype(edgelist|adjlist)
     * For specifying the number of shards, 20-50 million edges/shard is often a good configuration.
     */
    public static void main(String[] args) throws  Exception {
        String baseFilename = args[0];
        int nShards = Integer.parseInt(args[1]);
        String fileType = (args.length >= 3 ? args[2] : null);

        /* Create shards */
        FastSharder sharder = createSharder(baseFilename, nShards);
        if (baseFilename.equals("pipein")) {     // Allow piping graph in
            sharder.shard(System.in, fileType);
        } else {
            if (!new File(ChiFilenames.getFilenameIntervals(baseFilename, nShards)).exists()) {
                sharder.shard(new FileInputStream(new File(baseFilename)), fileType);
            } else {
                logger.info("Found shards -- no need to preprocess");
            }
        }

        /* Run GraphChi ... */
        GraphChiEngine<Integer, Integer> engine = new GraphChiEngine<Integer, Integer>(baseFilename, nShards);
        engine.setEdataConverter(new IntConverter());
        engine.setVertexDataConverter(new IntConverter());
        engine.setEnableScheduler(true);
        engine.run(new ConnectedComponents(), 5);

        logger.info("Ready. Going to output...");

        /* Process output. The output file has format <vertex-id, component-id> */
        LabelAnalysis.computeLabels(baseFilename, engine.numVertices(), engine.getVertexIdTranslate());

        logger.info("Finished. See file: " + baseFilename + ".components");
    }
}
