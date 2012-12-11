package edu.cmu.graphchi.preprocessing;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.LoggingInitializer;
import nom.tam.util.BufferedDataInputStream;

import java.io.*;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * New version of sharder that requires predefined number of shards
 * and translates the vertex ids in order to randomize the order.
 * Need to use VertexIdTranslate.  Requires enough memory to store vertex degrees (TODO, fix).
 */
public class FastSharder {

    private String baseFilename;
    private int numShards;
    private int initialIntervalLength;
    private VertexIdTranslate preIdTranslate;
    private VertexIdTranslate finalIdTranslate;

    private DataOutputStream[] shovelStreams;
    private int maxVertexId = 0;

    private int[] inDegrees;
    private int[] outDegrees;

    private static final Logger logger = LoggingInitializer.getLogger("fast-sharder");


    public FastSharder(String baseFilename, int numShards) throws IOException {
        this.baseFilename = baseFilename;
        this.numShards = numShards;
        this.initialIntervalLength = Integer.MAX_VALUE / numShards;
        this.preIdTranslate = new VertexIdTranslate(this.initialIntervalLength, numShards);

        shovelStreams = new DataOutputStream[numShards];
        for(int i=0; i < numShards; i++) {
           shovelStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(shovelFilename(i))));
        }
    }

    private String shovelFilename(int i) {
        return baseFilename + ".shovel." + i;
    }


    public void addEdge(int from, int to) throws IOException {
        int preTranslatedIdFrom = preIdTranslate.forward(from);
        int preTranslatedTo = preIdTranslate.forward(to);

        if (maxVertexId < from) maxVertexId = from;
        if (maxVertexId < to)  maxVertexId = to;

        addToShovel(preTranslatedIdFrom, preTranslatedTo);
    }

    private void addToShovel(int preTranslatedIdFrom, int preTranslatedTo) throws IOException {
        DataOutputStream strm = shovelStreams[preTranslatedTo % numShards];
        strm.writeLong(packEdges(preTranslatedIdFrom, preTranslatedTo));
    }

    public static long packEdges(int a, int b) {
        return ((long) a << 32) + b;
    }

    public static int getFirst(long l) {
       return  (int)  (l >> 32);
    }

    public static int getSecond(long l) {
        return (int) (l & 0x00000000ffffffffl);
    }


    public void process() throws  IOException {
        inDegrees = new int[maxVertexId + 1];
        outDegrees = new int[maxVertexId + 1];
        finalIdTranslate = new VertexIdTranslate((1 + maxVertexId) / numShards + 1, numShards);

        for(int i=0; i < numShards; i++) {
            shovelStreams[i].close();
        }
        shovelStreams = null;

        for(int i=0; i<numShards; i++) {
            processShovel(i);
        }
    }

    private void processShovel(int shardNum) throws IOException {
        File shovelFile = new File(shovelFilename(shardNum));
        long[] shoveled = new long[(int) (shovelFile.length() / 8)];

        logger.info("Processing shovel " + shardNum);

        BufferedDataInputStream in = new BufferedDataInputStream(new FileInputStream(shovelFile));
        for(int i=0; i<shoveled.length; i++) {
            long l = in.readLong();
            int from = getFirst(l);
            int to = getSecond(l);
            int newFrom = finalIdTranslate.forward(preIdTranslate.backward(from));
            int newTo = finalIdTranslate.backward(preIdTranslate.backward(to));
            shoveled[i] = packEdges(newFrom, newTo);
        }
        in.close();

        shovelFile.delete();
        Arrays.sort(shoveled);  // The source id is  higher order, so sorting the longs will produce right result

        File adjFile = new File(ChiFilenames.getFilenameShardsAdj(baseFilename, shardNum, numShards));

    }


}
