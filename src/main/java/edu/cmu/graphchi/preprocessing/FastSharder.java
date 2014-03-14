package edu.cmu.graphchi.preprocessing;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.auxdata.VertexData;
import edu.cmu.graphchi.io.CompressedIO;
import edu.cmu.graphchi.shards.MemoryShard;
import edu.cmu.graphchi.shards.SlidingShard;
import nom.tam.util.BufferedDataInputStream;

import java.io.*;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.zip.DeflaterOutputStream;

/**
 * New version of sharder that requires predefined number of shards
 * and translates the vertex ids in order to randomize the order, thus
 * requiring no additional step to divide the number of edges for
 * each shard equally (it is assumed that probablistically the number
 * of edges is roughly even).
 *
 * Since the vertex ids are translated to internal-ids, you need to use
 * VertexIdTranslate class to obtain the original id-numbers.
 *
 * Usage:
 * <code>
 *     FastSharder sharder = new FastSharder(graphName, numShards, ....)
 *     sharder.shard(new FileInputStream())
 * </code>
 *
 * To use a pipe to feed a graph, use
 * <code>
 *     sharder.shard(System.in, "edgelist");
 * </code>
 *
 * <b>Note:</b> <a href="http://code.google.com/p/graphchi/wiki/EdgeListFormat">Edge list</a>
 * and <a href="http://code.google.com/p/graphchi/wiki/AdjacencyListFormat">adjacency list</a>
 * formats are supported.
 *
 * <b>Note:</b>If from and to vertex ids equal (applies only to edge list format), the line is assumed to contain vertex-value.
 *
 * @author Aapo Kyrola
 */
public class FastSharder <VertexValueType, EdgeValueType> {

    public enum GraphInputFormat {EDGELIST, ADJACENCY, MATRIXMARKET};

    private String baseFilename;
    private int numShards;
    private int initialIntervalLength;
    private VertexIdTranslate preIdTranslate;
    private VertexIdTranslate finalIdTranslate;

    private DataOutputStream[] shovelStreams;
    private DataOutputStream[] vertexShovelStreams;

    private int maxVertexId = 0;

    private int[] inDegrees;
    private int[] outDegrees;
    private boolean memoryEfficientDegreeCount = false;
    private long numEdges = 0;
    private boolean useSparseDegrees = false;
    private boolean allowSparseDegreesAndVertexData = false;

    private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;
    private BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter;

    private EdgeProcessor<EdgeValueType> edgeProcessor;
    private VertexProcessor<VertexValueType> vertexProcessor;


    private static final Logger logger = ChiLogger.getLogger("fast-sharder");

    /**
     * Constructor
     * @param baseFilename input-file
     * @param numShards the number of shards to be created
     * @param vertexProcessor user-provided function for translating strings to vertex value type
     * @param edgeProcessor user-provided function for translating strings to edge value type
     * @param vertexValConterter translator  byte-arrays to/from vertex-value
     * @param edgeValConverter   translator  byte-arrays to/from edge-value
     * @throws IOException  if problems reading the data
     */
    public FastSharder(String baseFilename, int numShards,

                       VertexProcessor<VertexValueType> vertexProcessor,
                       EdgeProcessor<EdgeValueType> edgeProcessor,
                       BytesToValueConverter<VertexValueType> vertexValConterter,
                       BytesToValueConverter<EdgeValueType> edgeValConverter) throws IOException {
        this.baseFilename = baseFilename;
        this.numShards = numShards;
        this.initialIntervalLength = Integer.MAX_VALUE / numShards;
        this.preIdTranslate = new VertexIdTranslate(this.initialIntervalLength, numShards);
        this.edgeProcessor = edgeProcessor;
        this.vertexProcessor = vertexProcessor;
        this.edgeValueTypeBytesToValueConverter = edgeValConverter;
        this.vertexValueTypeBytesToValueConverter = vertexValConterter;

        /**
         * In the first phase of processing, the edges are "shoveled" to
         * the corresponding shards. The interim shards are called "shovel-files",
         * and the final shards are created by sorting the edges in the shovel-files.
         * See processShovel()
         */
        shovelStreams = new DataOutputStream[numShards];
        vertexShovelStreams = new DataOutputStream[numShards];
        for(int i=0; i < numShards; i++) {
            shovelStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(shovelFilename(i))));
            if (vertexProcessor != null) {
                vertexShovelStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(vertexShovelFileName(i))));
            }
        }

        /** Byte-array template used as a temporary value for performance (instead of
         *  always reallocating it).
         **/
        if (edgeValueTypeBytesToValueConverter != null) {
            valueTemplate =  new byte[edgeValueTypeBytesToValueConverter.sizeOf()];
        } else {
            valueTemplate = new byte[0];
        }
        if (vertexValueTypeBytesToValueConverter != null)
            vertexValueTemplate = new byte[vertexValueTypeBytesToValueConverter.sizeOf()];
    }

    private String shovelFilename(int i) {
        return baseFilename + ".shovel." + i;
    }

    private String vertexShovelFileName(int i) {
        return baseFilename + ".vertexshovel." + i;
    }


    /**
     * Adds an edge to the preprocessing.
     * @param from
     * @param to
     * @param edgeValueToken
     * @throws IOException
     */
    public void addEdge(int from, int to, String edgeValueToken) throws IOException {
        if (maxVertexId < from) maxVertexId = from;
        if (maxVertexId < to)  maxVertexId = to;

        /* If the from and to ids are same, this entry is assumed to contain value
           for the vertex, and it is passed to the vertexProcessor.
         */

        if (from == to) {
            if (vertexProcessor != null && edgeValueToken != null) {
                VertexValueType value = vertexProcessor.receiveVertexValue(from, edgeValueToken);
                if (value != null) {
                    addVertexValue(from % numShards, preIdTranslate.forward(from), value);
                }
            }
            return;
        }
        int preTranslatedIdFrom = preIdTranslate.forward(from);
        int preTranslatedTo = preIdTranslate.forward(to);

        addToShovel(to % numShards, preTranslatedIdFrom, preTranslatedTo,
                (edgeProcessor != null ? edgeProcessor.receiveEdge(from, to, edgeValueToken) : null));
    }


    private byte[] valueTemplate;
    private byte[] vertexValueTemplate;


    /**
     * Adds n edge to the shovel.  At this stage, the vertex-ids are "pretranslated"
     * to a temporary internal ids. In the last phase, each vertex-id is assigned its
     * final id. The pretranslation is requried because at this point we do not know
     * the total number of vertices.
     * @param shard
     * @param preTranslatedIdFrom internal from-id
     * @param preTranslatedTo internal to-id
     * @param value
     * @throws IOException
     */
    private void addToShovel(int shard, int preTranslatedIdFrom, int preTranslatedTo,
                             EdgeValueType value) throws IOException {
        DataOutputStream strm = shovelStreams[shard];
        strm.writeLong(packEdges(preTranslatedIdFrom, preTranslatedTo));
        if (edgeValueTypeBytesToValueConverter != null) {
            edgeValueTypeBytesToValueConverter.setValue(valueTemplate, value);
        }
        strm.write(valueTemplate);
    }


    public boolean isAllowSparseDegreesAndVertexData() {
        return allowSparseDegreesAndVertexData;
    }

    /**
     * If set true, GraphChi will use sparse file for vertices and the degree data
     * if the number of edges is smaller than the number of vertices. Default false.
     * Note: if you use this, you probably want to set engine.setSkipZeroDegreeVertices(true)
     * @param allowSparseDegreesAndVertexData
     */
    public void setAllowSparseDegreesAndVertexData(boolean allowSparseDegreesAndVertexData) {
        this.allowSparseDegreesAndVertexData = allowSparseDegreesAndVertexData;
    }

    /**
     * We keep separate shovel-file for vertex-values.
     * @param shard
     * @param pretranslatedVertexId
     * @param value
     * @throws IOException
     */
    private void addVertexValue(int shard, int pretranslatedVertexId, VertexValueType value) throws IOException{
        DataOutputStream strm = vertexShovelStreams[shard];
        strm.writeInt(pretranslatedVertexId);
        vertexValueTypeBytesToValueConverter.setValue(vertexValueTemplate, value);
        strm.write(vertexValueTemplate);
    }


    /**
     * Bit arithmetic for packing two 32-bit vertex-ids into one 64-bit long.
     * @param a
     * @param b
     * @return
     */
    static long packEdges(int a, int b) {
        return ((long) a << 32) + b;
    }

    static int getFirst(long l) {
        return  (int)  (l >> 32);
    }

    static int getSecond(long l) {
        return (int) (l & 0x00000000ffffffffl);
    }

    /**
     * Final processing after all edges have been received.
     * @throws IOException
     */
    public void process() throws IOException {
        /* Check if we have enough memory to keep track of
           vertex degree in memory. If not, we need to run a special
           graphchi-program to create the degree-file.
         */

        // Ad-hoc: require that degree vertices won't take more than 5th of memory
        memoryEfficientDegreeCount = Runtime.getRuntime().maxMemory() / 5 <  ((long) maxVertexId) * 8;

        if (memoryEfficientDegreeCount) {
            logger.info("Going to use memory-efficient, but slower, method to compute vertex degrees.");
        }

        if (!memoryEfficientDegreeCount) {
            inDegrees = new int[maxVertexId + numShards];
            outDegrees = new int[maxVertexId + numShards];
        }

        /**
         * Now when we have the total number of vertices known, we can
         * construct the final translator.
         */
        finalIdTranslate = new VertexIdTranslate((1 + maxVertexId) / numShards + 1, numShards);

        /**
         * Store information on how to translate internal vertex id to the original id.
         */
        saveVertexTranslate();

        /**
         * Close / flush each shovel-file.
         */
        for(int i=0; i < numShards; i++) {
            shovelStreams[i].close();
        }
        shovelStreams = null;

        /**
         *  Store the vertex intervals.
         */
        writeIntervals();

        /**
         * Process each shovel to create a final shard.
         */
        for(int i=0; i<numShards; i++) {
            processShovel(i);
        }

        /**
         * If we have more vertices than edges, it makes sense to use sparse representation
         * for the auxilliary degree-data and vertex-data files.
         */
        if (allowSparseDegreesAndVertexData) {
            useSparseDegrees = (maxVertexId > numEdges) || "1".equals(System.getProperty("sparsedeg"));
        } else {
            useSparseDegrees = false;
        }
        logger.info("Use sparse output: " + useSparseDegrees);

        /**
         * Construct the degree-data file which stores the in- and out-degree
         * of each vertex. See edu.cmu.graphchi.engine.auxdata.DegreeData
         */
        if (!memoryEfficientDegreeCount) {
            writeDegrees();
        } else {
            computeVertexDegrees();
        }

        /**
         * Write the vertex-data file.
         */
        if (vertexProcessor != null) {
            processVertexValues(useSparseDegrees);
        }
    }


    /**
     * Consteuct the degree-file if we had degrees computed in-memory,
     * @throws IOException
     */
    private void writeDegrees() throws IOException {
        DataOutputStream degreeOut = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(ChiFilenames.getFilenameOfDegreeData(baseFilename, useSparseDegrees))));
        for(int i=0; i<inDegrees.length; i++) {
            if (!useSparseDegrees)   {
                degreeOut.writeInt(Integer.reverseBytes(inDegrees[i]));
                degreeOut.writeInt(Integer.reverseBytes(outDegrees[i]));
            } else {
                if (inDegrees[i] + outDegrees[i] > 0) {
                    degreeOut.writeInt(Integer.reverseBytes(i));
                    degreeOut.writeInt(Integer.reverseBytes(inDegrees[i]));
                    degreeOut.writeInt(Integer.reverseBytes(outDegrees[i]));
                }
            }
        }
        degreeOut.close();
    }

    private void writeIntervals() throws IOException{
        FileWriter wr = new FileWriter(ChiFilenames.getFilenameIntervals(baseFilename, numShards));
        for(int j=1; j<=numShards; j++) {
            int a =(j * finalIdTranslate.getVertexIntervalLength() -1);
            wr.write(a + "\n");
            if (a > maxVertexId) {
                maxVertexId = a;
            }
        }
        wr.close();
    }

    private void saveVertexTranslate() throws IOException {
        FileWriter wr = new FileWriter(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards));
        wr.write(finalIdTranslate.stringRepresentation());
        wr.close();
    }

    /**
     * Initializes the vertex-data file. Similar process as sharding for edges.
     * @param sparse
     * @throws IOException
     */
    private void processVertexValues(boolean sparse) throws IOException {
        DataBlockManager dataBlockManager = new DataBlockManager();
        VertexData<VertexValueType> vertexData = new VertexData<VertexValueType>(maxVertexId + 1, baseFilename,
                vertexValueTypeBytesToValueConverter, sparse);
        vertexData.setBlockManager(dataBlockManager);
        for(int p=0; p < numShards; p++) {
            int intervalSt = p * finalIdTranslate.getVertexIntervalLength();
            int intervalEn = (p + 1) * finalIdTranslate.getVertexIntervalLength() - 1;
            if (intervalEn > maxVertexId) intervalEn = maxVertexId;

            vertexShovelStreams[p].close();

            /* Read shovel and sort */
            File shovelFile = new File(vertexShovelFileName(p));
            BufferedDataInputStream in = new BufferedDataInputStream(new FileInputStream(shovelFile));

            int sizeOf = vertexValueTypeBytesToValueConverter.sizeOf();
            long[] vertexIds = new long[(int) (shovelFile.length() / (4 + sizeOf))];
            if (vertexIds.length == 0) continue;
            byte[] vertexValues = new byte[vertexIds.length * sizeOf];
            for(int i=0; i<vertexIds.length; i++) {
                int vid = in.readInt();
                int transVid = finalIdTranslate.forward(preIdTranslate.backward(vid));
                vertexIds[i] = transVid;
                in.readFully(vertexValueTemplate);
                int valueIdx = i * sizeOf;
                System.arraycopy(vertexValueTemplate, 0, vertexValues, valueIdx, sizeOf);
            }

            /* Sort */
            sortWithValues(vertexIds, vertexValues, sizeOf);  // The source id is  higher order, so sorting the longs will produce right result

            int SUBINTERVAL = 2000000;

            int iterIdx = 0;

            /* Insert into data */
            for(int subIntervalSt=intervalSt; subIntervalSt < intervalEn; subIntervalSt += SUBINTERVAL) {
                int subIntervalEn = subIntervalSt + SUBINTERVAL - 1;
                if (subIntervalEn > intervalEn) subIntervalEn = intervalEn;
                int blockId = vertexData.load(subIntervalSt, subIntervalEn);

                Iterator<Integer> iterator = vertexData.currentIterator();
                while(iterator.hasNext()) {
                    int curId = iterator.next();

                    while(iterIdx < vertexIds.length && vertexIds[iterIdx] < curId) {
                        iterIdx++;
                    }
                    if (iterIdx >= vertexIds.length) break;

                    if (curId == (int) vertexIds[iterIdx]) {
                        ChiPointer pointer = vertexData.getVertexValuePtr(curId, blockId);
                        System.arraycopy(vertexValues, iterIdx * sizeOf, vertexValueTemplate, 0, sizeOf);
                        dataBlockManager.writeValue(pointer, vertexValueTemplate);
                    } else {
                        // No vertex data for that vertex.
                    }

                }
                vertexData.releaseAndCommit(subIntervalSt, blockId);
            }
        }
    }

    /**
     * Converts a shovel-file into a shard.
     * @param shardNum
     * @throws IOException
     */
    private void processShovel(int shardNum) throws IOException {
        File shovelFile = new File(shovelFilename(shardNum));
        int sizeOf = (edgeValueTypeBytesToValueConverter != null ? edgeValueTypeBytesToValueConverter.sizeOf() : 0);

        long[] shoveled = new long[(int) (shovelFile.length() / (8 + sizeOf))];

        // TODO: improve
        if (shoveled.length > 500000000) {
            throw new RuntimeException("Too big shard size, shovel length was: " + shoveled.length + " max: " + 500000000);
        }
        byte[] edgeValues = new byte[shoveled.length * sizeOf];


        logger.info("Processing shovel " + shardNum);

        /**
         * Read the edges into memory.
         */
        BufferedDataInputStream in = new BufferedDataInputStream(new FileInputStream(shovelFile));
        for(int i=0; i<shoveled.length; i++) {
            long l = in.readLong();
            int from = getFirst(l);
            int to = getSecond(l);
            in.readFully(valueTemplate);

            int newFrom = finalIdTranslate.forward(preIdTranslate.backward(from));
            int newTo = finalIdTranslate.forward(preIdTranslate.backward(to));
            shoveled[i] = packEdges(newFrom, newTo);

            /* Edge value */
            int valueIdx = i * sizeOf;
            System.arraycopy(valueTemplate, 0, edgeValues, valueIdx, sizeOf);
            if (!memoryEfficientDegreeCount) {
                inDegrees[newTo]++;
                outDegrees[newFrom]++;
            }
        }
        numEdges += shoveled.length;

        in.close();

        /* Delete the shovel-file */
        shovelFile.delete();

        logger.info("Processing shovel " + shardNum + " ... sorting");

        /* Sort the edges */
        sortWithValues(shoveled, edgeValues, sizeOf);  // The source id is  higher order, so sorting the longs will produce right result

        logger.info("Processing shovel " + shardNum + " ... writing shard");


        /*
         Now write the final shard in a compact form. Note that there is separate shard
         for adjacency and the edge-data. The edge-data is split and stored into 4-megabyte compressed blocks.
         */

        /**
         * Step 1: ADJACENCY SHARD
         */
        File adjFile = new File(ChiFilenames.getFilenameShardsAdj(baseFilename, shardNum, numShards));
        DataOutputStream adjOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(adjFile)));
        File indexFile = new File(adjFile.getAbsolutePath() + ".index");
        DataOutputStream indexOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)));
        int curvid = 0;
        int istart = 0;
        int edgeCounter = 0;
        int lastIndexFlush = 0;
        int edgesPerIndexEntry = 4096; // Tuned for fast shard queries

        for(int i=0; i <= shoveled.length; i++) {
            int from = (i < shoveled.length ? getFirst(shoveled[i]) : -1);

            if (from != curvid) {
                /* Write index */
                if (edgeCounter - lastIndexFlush >= edgesPerIndexEntry) {
                    indexOut.writeInt(curvid);
                    indexOut.writeInt(adjOut.size());
                    indexOut.writeInt(edgeCounter);
                    lastIndexFlush = edgeCounter;
                }

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
                    edgeCounter++;
                }

                istart = i;

                // Handle zeros
                if (from != (-1)) {
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
                }
                curvid = from;
            }
        }
        adjOut.close();
        indexOut.close();



        /**
         * Step 2: EDGE DATA
         */

        /* Create compressed edge data directories */
        if (sizeOf > 0) {
            int blockSize = ChiFilenames.getBlocksize(sizeOf);
            String edataFileName = ChiFilenames.getFilenameShardEdata(baseFilename, new BytesToValueConverter() {
                @Override
                public int sizeOf() {
                    return edgeValueTypeBytesToValueConverter.sizeOf();
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

            long edatasize = shoveled.length * edgeValueTypeBytesToValueConverter.sizeOf();
            FileWriter sizeWr = new FileWriter(edgeDataSizeFile);
            sizeWr.write(edatasize + "");
            sizeWr.close();

            /* Create compressed blocks */
            int blockIdx = 0;
            int edgeIdx= 0;
            for(long idx=0; idx < edatasize; idx += blockSize) {
                File blockFile = new File(ChiFilenames.getFilenameShardEdataBlock(edataFileName, blockIdx, blockSize));
                OutputStream blockOs = (CompressedIO.isCompressionEnabled() ?
                        new DeflaterOutputStream(new BufferedOutputStream(new FileOutputStream(blockFile))) :
                        new FileOutputStream(blockFile));
                long len = Math.min(blockSize, edatasize - idx);
                byte[] block = new byte[(int)len];

                System.arraycopy(edgeValues, edgeIdx * sizeOf, block, 0, block.length);
                edgeIdx += len / sizeOf;

                blockOs.write(block);
                blockOs.close();
                blockIdx++;
            }

            assert(edgeIdx == edgeValues.length);
        }
    }

    private static Random random = new Random();



    // http://www.algolist.net/Algorithms/Sorting/Quicksort
    // TODO: implement faster
    private static int partition(long arr[], byte[] values, int sizeOf, int left, int right)
    {
        int i = left, j = right;
        long tmp;
        long pivot = arr[left + random.nextInt(right - left + 1)];
        byte[] valueTemplate = new byte[sizeOf];

        while (i <= j) {
            while (arr[i] < pivot)
                i++;
            while (arr[j] > pivot)
                j--;
            if (i <= j) {
                tmp = arr[i];

                /* Swap */
                System.arraycopy(values, j * sizeOf, valueTemplate, 0, sizeOf);
                System.arraycopy(values, i * sizeOf, values, j * sizeOf, sizeOf);
                System.arraycopy(valueTemplate, 0, values, i * sizeOf, sizeOf);

                arr[i] = arr[j];
                arr[j] = tmp;
                i++;
                j--;
            }
        }

        return i;
    }

    static void quickSort(long arr[], byte[] values, int sizeOf, int left, int right) {
        if (left < right) {
            int index = partition(arr, values, sizeOf, left, right);
            if (left < index - 1)
                quickSort(arr, values, sizeOf, left, index - 1);
            if (index < right)
                quickSort(arr, values, sizeOf, index, right);
        }
    }


    public static void sortWithValues(long[] shoveled, byte[] edgeValues, int sizeOf) {
        quickSort(shoveled, edgeValues, sizeOf, 0, shoveled.length - 1);
    }


    /**
     * Execute sharding by reading edges from a inputstream
     * @param inputStream
     * @param format graph input format
     * @throws IOException
     */
    public void shard(InputStream inputStream, GraphInputFormat format) throws IOException {
        BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
        String ln;
        long lineNum = 0;


        if (!format.equals(GraphInputFormat.MATRIXMARKET)) {
            while ((ln = ins.readLine()) != null) {
                if (ln.length() > 2 && !ln.startsWith("#")) {
                    lineNum++;
                    if (lineNum % 2000000 == 0) logger.info("Reading line: " + lineNum);

                    String[] tok = ln.split("\t");
                    if (tok.length == 1) tok = ln.split(" ");

                    if (tok.length > 1) {
                        if (format == GraphInputFormat.EDGELIST) {
                        /* Edge list: <src> <dst> <value> */
                            if (tok.length == 2) {
                                this.addEdge(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]), null);
                            } else if (tok.length == 3) {
                                this.addEdge(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]), tok[2]);
                            }
                        } else if (format == GraphInputFormat.ADJACENCY) {
                        /* Adjacency list: <vertex-id> <count> <neighbor-1> <neighbor-2> ... */
                            int vertexId = Integer.parseInt(tok[0]);
                            int len = Integer.parseInt(tok[1]);
                            if (len != tok.length - 2) {
                                if (lineNum < 10) {
                                    throw new IllegalArgumentException("Error on line " + lineNum + "; number of edges does not match number of tokens:" +
                                            len + " != " + tok.length);
                                } else {
                                    logger.warning("Error on line " + lineNum + "; number of edges does not match number of tokens:" +
                                            len + " != " + tok.length);
                                    break;
                                }
                            }
                            for(int j=2; j < 2 + len; j++) {
                                int dest = Integer.parseInt(tok[j]);
                                this.addEdge(vertexId, dest, null);
                            }
                        } else {
                            throw new IllegalArgumentException("Please specify graph input format");
                        }
                    }
                }
            }
        } else if (format.equals(GraphInputFormat.MATRIXMARKET)) {
            /* Process matrix-market format to create a bipartite graph. */
            boolean parsedMatrixSize = false;
            int numLeft = 0;
            int numRight = 0;
            long totalEdges = 0;
            while ((ln = ins.readLine()) != null) {
                lineNum++;
                if (ln.length() > 2 && !ln.startsWith("#")) {
                    if (ln.startsWith("%%")) {
                        if (!ln.contains(("matrix coordinate real general"))) {
                            throw new RuntimeException("Unknown matrix market format!");
                        }
                    } else if (ln.startsWith("%")) {
                        // Comment - skip
                    } else {
                        String[] tok = ln.split(" ");
                        if (lineNum % 2000000 == 0) logger.info("Reading line: " + lineNum + " / " + totalEdges);
                        if (!parsedMatrixSize) {
                            numLeft = Integer.parseInt(tok[0]);
                            numRight = Integer.parseInt(tok[1]);
                            totalEdges = Long.parseLong(tok[2]);
                            logger.info("Matrix-market: going to load total of " + totalEdges + " edges.");
                            parsedMatrixSize = true;
                        } else {
                            /* The ids start from 1, so we take 1 off. */
                            /* Vertex - ids on the right side of the bipartite graph have id numLeft + originalId */
                            try {
                                String lastTok = tok[tok.length - 1];
                                this.addEdge(Integer.parseInt(tok[0]) - 1, numLeft + Integer.parseInt(tok[1]) - 1, lastTok);
                            } catch (NumberFormatException nfe) {
                                logger.severe("Could not parse line: " + ln);
                                throw nfe;
                            }
                        }
                    }
                }
            }

            /* Store matrix dimensions */
            String matrixMarketInfoFile = baseFilename + ".matrixinfo";
            FileOutputStream fos = new FileOutputStream(new File(matrixMarketInfoFile));
            fos.write((numLeft + "\t" + numRight + "\t" + totalEdges + "\n").getBytes());
            fos.close();
        }



        this.process();
    }

    /**
     * Shard a graph
     * @param inputStream
     * @param format "edgelist" or "adjlist" / "adjacency"
     * @throws IOException
     */
    public void shard(InputStream inputStream, String format) throws IOException {
        if (format == null || format.equals("edgelist")) {
            shard(inputStream, GraphInputFormat.EDGELIST);
        }
        else if (format.equals("adjlist") || format.startsWith("adjacency")) {
            shard(inputStream, GraphInputFormat.ADJACENCY);
        }
    }

    /**
     * Shard an input graph with edge list format.
     * @param inputStream
     * @throws IOException
     */
    public void shard(InputStream inputStream) throws IOException {
        shard(inputStream, GraphInputFormat.EDGELIST);
    }

    /**
     * Compute vertex degrees by running a special graphchi program.
     * This is done only if we do not have enough memory to keep track of
     * vertex degrees in-memory.
     */
    private void computeVertexDegrees() {
        try {
            logger.info("Use sparse degrees: " + useSparseDegrees);

            DataOutputStream degreeOut = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(ChiFilenames.getFilenameOfDegreeData(baseFilename, useSparseDegrees))));


            SlidingShard[] slidingShards = new SlidingShard[numShards];
            for(int p=0; p < numShards; p++) {
                int intervalSt = p * finalIdTranslate.getVertexIntervalLength();
                int intervalEn = (p + 1) * finalIdTranslate.getVertexIntervalLength() - 1;

                slidingShards[p] = new SlidingShard(null, ChiFilenames.getFilenameShardsAdj(baseFilename, p, numShards),
                        intervalSt, intervalEn);
                slidingShards[p].setOnlyAdjacency(true);
            }

            int SUBINTERVAL = 2000000;
            ExecutorService parallelExecutor = Executors.newFixedThreadPool(4);

            for(int p=0; p < numShards; p++) {
                logger.info("Degree computation round " + p + " / " + numShards);
                int intervalSt = p * finalIdTranslate.getVertexIntervalLength();
                int intervalEn = (p + 1) * finalIdTranslate.getVertexIntervalLength() - 1;

                MemoryShard<Float> memoryShard = new MemoryShard<Float>(null, ChiFilenames.getFilenameShardsAdj(baseFilename, p, numShards),
                        intervalSt, intervalEn);
                memoryShard.setOnlyAdjacency(true);


                for(int subIntervalSt=intervalSt; subIntervalSt < intervalEn; subIntervalSt += SUBINTERVAL) {
                    int subIntervalEn = subIntervalSt + SUBINTERVAL - 1;
                    if (subIntervalEn > intervalEn) subIntervalEn = intervalEn;
                    ChiVertex[] verts = new ChiVertex[subIntervalEn - subIntervalSt + 1];
                    for(int i=0; i < verts.length; i++) {
                        verts[i] = new ChiVertex(i + subIntervalSt, null);
                    }

                    memoryShard.loadVertices(subIntervalSt, subIntervalEn, verts, false, parallelExecutor);
                    for(int i=0; i < numShards; i++) {
                        if (i != p) {
                            slidingShards[i].readNextVertices(verts, subIntervalSt, true);
                        }
                    }

                    for(int i=0; i < verts.length; i++) {
                        if (!useSparseDegrees) {
                            degreeOut.writeInt(Integer.reverseBytes(verts[i].numInEdges()));
                            degreeOut.writeInt(Integer.reverseBytes(verts[i].numOutEdges()));
                        } else {
                            if (verts[i].numEdges() > 0 ){
                                degreeOut.writeInt(Integer.reverseBytes(subIntervalSt + i));
                                degreeOut.writeInt(Integer.reverseBytes(verts[i].numInEdges()));
                                degreeOut.writeInt(Integer.reverseBytes(verts[i].numOutEdges()));
                            }
                        }
                    }
                }
            }
            parallelExecutor.shutdown();
            degreeOut.close();
        } catch (Exception err) {
            err.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        String fileName = args[0];
        int numShards = Integer.parseInt(args[1]);
        String conversion = args[2];
        FastSharder<Integer, Integer> sharder = new FastSharder<Integer, Integer>(fileName, numShards, null, new EdgeProcessor<Integer>() {
            @Override
            public Integer receiveEdge(int from, int to, String token) {
                if (token == null) return 0;
                return Integer.parseInt(token);
            }
        },
                new IntConverter(), new IntConverter());
        sharder.shard(new FileInputStream(fileName), conversion);

    }
}
