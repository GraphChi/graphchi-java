package edu.cmu.graphchi.preprocessing;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.LoggingInitializer;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import nom.tam.util.BufferedDataInputStream;

import java.io.*;
import java.util.Arrays;
import java.util.logging.Logger;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

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
    private final int edgeDataSize;

    private static final Logger logger = LoggingInitializer.getLogger("fast-sharder");


    public FastSharder(String baseFilename, int numShards, int edgeDataSize) throws IOException {
        this.baseFilename = baseFilename;
        this.numShards = numShards;
        this.initialIntervalLength = Integer.MAX_VALUE / numShards;
        this.preIdTranslate = new VertexIdTranslate(this.initialIntervalLength, numShards);

        shovelStreams = new DataOutputStream[numShards];
        for(int i=0; i < numShards; i++) {
            shovelStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(shovelFilename(i))));
        }
        this.edgeDataSize = edgeDataSize;
    }

    private String shovelFilename(int i) {
        return baseFilename + ".shovel." + i;
    }


    public void addEdge(int from, int to) throws IOException {
        if (from == to) return;
        int preTranslatedIdFrom = preIdTranslate.forward(from);
        int preTranslatedTo = preIdTranslate.forward(to);

        if (maxVertexId < from) maxVertexId = from;
        if (maxVertexId < to)  maxVertexId = to;

        addToShovel(to % numShards, preTranslatedIdFrom, preTranslatedTo);
    }

    private void addToShovel(int shard, int preTranslatedIdFrom, int preTranslatedTo) throws IOException {
        DataOutputStream strm = shovelStreams[shard];
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
        inDegrees = new int[maxVertexId + numShards];
        outDegrees = new int[maxVertexId + numShards];
        finalIdTranslate = new VertexIdTranslate((1 + maxVertexId) / numShards + 1, numShards);

        for(int i=0; i < numShards; i++) {
            shovelStreams[i].close();
        }
        shovelStreams = null;

        writeIntervals();

        for(int i=0; i<numShards; i++) {
            processShovel(i);
        }

        writeDegrees();
    }

    private void writeDegrees() throws IOException {
        DataOutputStream degreeOut = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(ChiFilenames.getFilenameOfDegreeData(baseFilename))));
        for(int i=0; i<inDegrees.length; i++) {
            degreeOut.writeInt(Integer.reverseBytes(inDegrees[i]));
            degreeOut.writeInt(Integer.reverseBytes(outDegrees[i]));
       }
        degreeOut.close();
    }

    private void writeIntervals() throws IOException{
        FileWriter wr = new FileWriter(ChiFilenames.getFilenameIntervals(baseFilename, numShards));
        for(int j=1; j<=numShards; j++) {
            wr.write((j * finalIdTranslate.getVertexIntervalLength() -1) + "\n");
        }
        wr.close();
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
            int newTo = finalIdTranslate.forward(preIdTranslate.backward(to));
            shoveled[i] = packEdges(newFrom, newTo);

            inDegrees[newTo]++;
            outDegrees[newFrom]++;
        }
        in.close();

        shovelFile.delete();

        logger.info("Processing shovel " + shardNum + " ... sorting");

        Arrays.sort(shoveled);  // The source id is  higher order, so sorting the longs will produce right result

        logger.info("Processing shovel " + shardNum + " ... writing shard");


        File adjFile = new File(ChiFilenames.getFilenameShardsAdj(baseFilename, shardNum, numShards));
        DataOutputStream adjOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(adjFile)));
        int curvid = 0;
        int istart = 0;
        for(int i=0; i < shoveled.length; i++) {
            int from = getFirst(shoveled[i]);


            if (from != curvid || i == shoveled.length - 1) {
                int count = i - istart;
                if (count > 0) {
                    if (count < 255) {
                        adjOut.writeByte(count);
                    } else {
                        adjOut.writeByte(0xff);
                        adjOut.writeInt(Integer.reverseBytes(count));
                    }
                }
                for(int j=istart; j<i; j++) {
                    adjOut.writeInt(Integer.reverseBytes(getSecond(shoveled[j])));
                }

                istart = i;

                // Handle zeros
                if (from - curvid > 1 || (i == 0 && from > 0)) {
                    int nz = from - curvid - 1;
                    if (i ==0 && from >0) nz = from;
                    do {
                        adjOut.writeByte(0);
                        nz--;
                        int tnz = Math.min(254, nz);
                        adjOut.writeByte(tnz);
                        nz -= tnz;
                    } while (nz > 0);
                }
                curvid = from;
            }
        }
        adjOut.close();

        /* Create compressed edge data directories */
        int blockSize = ChiFilenames.getBlocksize(edgeDataSize);


        String edataFileName = ChiFilenames.getFilenameShardEdata(baseFilename, new BytesToValueConverter() {
            @Override
            public int sizeOf() {
                return edgeDataSize;
            }

            @Override
            public Object getValue(byte[] array) {
                return null;
            }

            @Override
            public void setValue(byte[] array, Object val) {
            }
        }, shardNum, numShards);
        File edgeDataSizeFile = new File(edataFileName + ".size");
        File edgeDataDir = new File(ChiFilenames.getDirnameShardEdataBlock(edataFileName, blockSize));
        if (!edgeDataDir.exists()) edgeDataDir.mkdir();

        long edatasize = shoveled.length * edgeDataSize;
        FileWriter sizeWr = new FileWriter(edgeDataSizeFile);
        sizeWr.write(edatasize + "");
        sizeWr.close();

        /* Create blocks */
        int blockIdx = 0;
        for(long idx=0; idx < edatasize; idx += blockSize) {
            File blockFile = new File(ChiFilenames.getFilenameShardEdataBlock(edataFileName, blockIdx, blockSize));
            DeflaterOutputStream blockOs = new DeflaterOutputStream(new BufferedOutputStream(new FileOutputStream(blockFile)));
            long len = Math.min(blockSize, edatasize - idx);
            byte[] block = new byte[(int)len];
            blockOs.write(block);
            blockOs.close();
            blockIdx++;
        }
    }


}
