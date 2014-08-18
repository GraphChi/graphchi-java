package edu.cmu.graphchi.apps.kcore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
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
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.preprocessing.VertexProcessor;
import edu.cmu.graphchi.util.IdInt;
import edu.cmu.graphchi.util.Toplist;

/**
 * K-core decomposition algorithm
 *
 * Outputs: a file containing key-value pairs: vertexId, coreness
 *
 * How does it work ?
 * 1 - Initializes vertex values to their degrees then those values are communicated to neighbors. 
 * 2 - for each vertex v, an upper-bound is computed on its coreness based on the values received from neighbors.
 * 3 - if the upper-bound is better than its current value, v updates its value with the upper-bound.
 * 4 - Steps 2 and 3 are repeated until no more value updates are occurring.
 *
 * For correct results, run your input graph through GraphTransformer first.
 * Also, make sure to delete the preprocessed shard files created by GraphTransformer prior to running KCoreDecomposer.
 *
 * KCoreDecomposer is inspired from the algorithm presented in the following paper:
 * 		Distributed K-Core Decomposition
 * 		Alberto Montresor, Francesco De Pellegrini, Daniele Miorandi
 * 		http://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber=6189336
 *
 * <i>Note</i>: You may change output and input directory path based on your needs.
 *
 * @author Wissam Khaouid, wissamk@uvic.ca, 2014
 */

public class KCoreDecomposer implements GraphChiProgram<Integer, Integer> {

    public static final int INFINITY = Integer.MAX_VALUE;

    protected int vertexValuesUpdated;
    protected static int nVertexes = 0;

    private static int nIterations = 0;
    protected static BufferedWriter bw;

    private static Logger logger = ChiLogger.getLogger("kCoreDecomposition");

    public static void startWriting(File file, boolean append) throws IOException {
        FileWriter fw = new FileWriter(file, append);
        bw = new BufferedWriter(fw);
    }

    public static void stopWriting() throws IOException {
        bw.close();
    }

    public void update(ChiVertex<Integer, Integer> vertex, GraphChiContext context) {

        int iteration = context.getIteration();
        int numOutEdges = vertex.numOutEdges();

        if (iteration == 0) {
            vertex.setValue(numOutEdges);
            broadcastValue(vertex, numOutEdges);
            nVertexes++;
            vertexValuesUpdated++;
        } else {
            int topDrawer = vertex.getValue() + 1,
                    topDrawerCount = 0,
                    localEstimate = 0;

            SortedMap<Integer, Integer> inEdgeValueCounts =
                    Collections.synchronizedSortedMap(
                            new TreeMap<Integer, Integer>(Collections.reverseOrder()));

            for(int i = 0; i <= vertex.numOutEdges(); i++) {
                inEdgeValueCounts.put(i, 0);
            }

            for(int i = 0; i < vertex.numInEdges(); i++) {
                int inEdgeValue = vertex.inEdge(i).getValue();
                if( inEdgeValue >= topDrawer ) {
                    topDrawerCount ++;
                } else {
                    try {
                        int currentValue = inEdgeValueCounts.get(inEdgeValue);
                        inEdgeValueCounts.put(inEdgeValue, currentValue + 1);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                        System.exit(0);
                    }

                }
            }

            inEdgeValueCounts.put(topDrawer, topDrawerCount);
            localEstimate = computeLeastValue(inEdgeValueCounts);

            if( localEstimate < vertex.getValue() ) {
                vertex.setValue(localEstimate);
                broadcastValue(vertex, localEstimate);
                vertexValuesUpdated ++;
            }
        }

        context.getScheduler().addTask(vertex.getId());

    }

    /**
     * Computes the greatest x among a list of values, such that at least x values are greater than x
     * For now, the array is instantiated and filled up elsewhere
     */
    public int computeLeastValue(SortedMap<Integer, Integer> map) {
        int cumulCount = 0;
        int key, count;
        for(Map.Entry<Integer, Integer> entry : map.entrySet()) {
            key = entry.getKey();
            count = entry.getValue();
            cumulCount += count;
            if(cumulCount >= key) {
                return key;
            }
        }
        return 1;
    }

    /**
     * Broadcasts a value to the neighbors by writing it to the out-edges
     */

    public void broadcastValue(ChiVertex<Integer, Integer> vertex, int value) {
        for(int i = 0; i < vertex.numOutEdges(); i++) {
            vertex.outEdge(i).setValue(value);
        }
    }

    /**
     * Invoked with the start of a new iteration
     */
    public void beginIteration(GraphChiContext ctx) {
        vertexValuesUpdated = 0;
    }

    /**
     * Invoked at the end of every iteration
     */
    public void endIteration(GraphChiContext ctx) {
        System.out.println(vertexValuesUpdated + " updates.");
        System.out.println("iteration " + ctx.getIteration() + " ends.");

        nIterations ++;
        if( vertexValuesUpdated == 0 ) {
            System.out.println("no updates in this round. No more rounds .. KCore-montresor terminates!");
            ctx.getScheduler().removeAllTasks();
        }
    }

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

        /** Run from command line (Example)
         *	java -Xmx2048m -cp bin:gchi-libs/* -Dnum_threads=4 edu.cmu.graphchi.apps.kcore.KCoreDecomposition filename nbrOfShards filetype memoryBudget
         *
         * Assuming GraphChi jar files are saved in ./gchi-libs/
         */

        String inputDirectory = "./datasets/";
        String outputDirectory = "./output/";

        String fileName = args[0];
        int nShards = Integer.parseInt(args[1]);
        String fileType = args[2];
        int memBudget = (args.length >= 4 ? Integer.parseInt(args[3]) : null);

        CompressedIO.disableCompression();

        String inputFilePath = inputDirectory + fileName;
        
        /* Preprocessing graph : Making shards */

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

        /* Running GraphChi */
        GraphChiEngine<Integer, Integer> engine =
                new GraphChiEngine<Integer, Integer>(inputFilePath, nShards);
        engine.setMemoryBudgetMb(memBudget);
        engine.setSkipZeroDegreeVertices(true);
        engine.setEnableScheduler(true);
        engine.setEdataConverter(new IntConverter());
        engine.setVertexDataConverter(new IntConverter());

        engine.run(new KCoreDecomposer(), INFINITY);

        logger.info("Ready.");

        /* Outputting Core Values */
        startWriting(new File(outputDirectory + "out-cores-" + fileName), false);
        bw.write(nVertexes + "\n");

        VertexIdTranslate trans = engine.getVertexIdTranslate();
        TreeSet<IdInt> topToBottom = Toplist.topListInt(inputFilePath,
                engine.numVertices(), engine.numVertices());

        for(IdInt walker : topToBottom) {
            float coreValue = walker.getValue();
            bw.write(trans.backward(walker.getVertexId()) + ", " + String.valueOf((int)coreValue) + "\n");
        }

        stopWriting();

        System.out.println("Vertexes Processed: " + engine.numVertices());
        System.out.println("Edges Processed: " + engine.numEdges()) ;

        System.out.println("nIterations: " + nIterations);
        System.out.println("Success!");

    }

}
