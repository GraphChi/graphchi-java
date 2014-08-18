package edu.cmu.graphchi.apps.kcore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.io.CompressedIO;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexProcessor;

/**
 * Converts an indirected input graph into a directed by checking that each edge has a complimentary edge
 * in the opposite direction, and adding those complimentary edges when applicable.
 *
 * <i>Note</i>: You may change output and input directory path based on your needs.
 *
 * @author Wissam Khaouid, wissamk@uvic.ca, 2014
 */

public class GraphTransformer implements GraphChiProgram<Integer, Integer> {

    protected static int nEdgesAdded = 0;

    protected static BufferedWriter bw;

    private static Logger logger = ChiLogger.getLogger("GraphConverter");

    public static void startWriting(File file, boolean append) throws IOException {
        FileWriter fw = new FileWriter(file, append);
        bw = new BufferedWriter(fw);
    }

    public static void stopWriting() throws IOException {
        bw.close();
    }

    public void update(ChiVertex<Integer, Integer> vertex, GraphChiContext context) {
        ArrayList<Integer> outNeighbors = new ArrayList<Integer>();

        for(int i = 0; i < vertex.numOutEdges(); i++) {
            outNeighbors.add(vertex.outEdge(i).getVertexId());
        }

        for(int i = 0; i < vertex.numInEdges(); i++) {
            if (!outNeighbors.contains(vertex.inEdge(i).getVertexId()) ) {
                try {
                    bw.write("\n" + vertex.getId() + "\t" + vertex.inEdge(i).getVertexId());
                    nEdgesAdded ++;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void beginIteration(GraphChiContext ctx) {}

    public void endIteration(GraphChiContext ctx) {}

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {}

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

    public static void main(String[] args) throws IOException {

        /**
         * java -Xmx2048m -cp bin:gchi-libs/* -Dnum_threads=8  edu.cmu.graphchi.apps.kcore.GraphTransformer filename nbrOfShards filetype memoryBudget
         */

        String inputDirectory = "./datasets/";
        String outputDirectory = "./output/";

        String fileName = args[0];
        int nShards = Integer.parseInt(args[1]);
        String fileType = args[2];
        int memBudget = (args.length >= 4 ? Integer.parseInt(args[3]) : null);

        CompressedIO.disableCompression();

        String inputFilePath = inputDirectory + fileName;
        
        /* Making shards */
        FastSharder sharder = createSharder(inputFilePath, nShards);
        if (inputFilePath.equals("pipein")) {     // Allow piping graph in
            sharder.shard(System.in, fileType);
        } else {
            if (!new File(ChiFilenames.getFilenameIntervals(inputFilePath, nShards)).exists()) {
                sharder.shard(new FileInputStream(new File(inputFilePath)), fileType);
            } else {
                logger.info("Found shards -- no need to preprocess");
            }
        }
        
        /* Complementary edges will be appended to the input graph file throughout execution */
        startWriting(new File(inputFilePath), true);

        /* Running GraphChi */
        GraphChiEngine<Integer, Integer> engine = new GraphChiEngine<Integer, Integer>(inputFilePath, nShards);
        engine.setMemoryBudgetMb(memBudget);
        engine.setSkipZeroDegreeVertices(true);
        engine.setEnableScheduler(true);
        engine.setEdataConverter(new IntConverter());
        engine.setVertexDataConverter(new IntConverter());

        engine.run(new GraphTransformer(), 1);

        stopWriting();
        
        /* Write report file */
        startWriting(new File(outputDirectory + "gtransformer-report-" + fileName), false);
        bw.write("Total edges added: " + nEdgesAdded + "\n");
        stopWriting();

        logger.info("Success!");

    }

}



