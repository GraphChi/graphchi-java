package com.twitter.pers.graphchi.walks;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import edu.cmu.graphchi.Scheduler;
import edu.cmu.graphchi.engine.VertexInterval;

import java.io.*;
import java.lang.management.MemoryManagerMXBean;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Manager for random walks
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
public class WalkManager {

    private static final int MAX_SOURCES = 16777216;

    private final static int bucketSize = 128; // Store walks into buckets for faster retrieval
    private final static int initialSize = Integer.parseInt(System.getProperty("walkmanager.initial_size", "128"));

    private int sourceSeqIdx  = 0;
    private int[] sources;

    private int[] sourceWalkCounts = null;
    private long totalWalks = 0;

    private int[][] walks;
    private Object[] bucketLocks;
    private int[] walkIndices;
    private BitSet sourceBitSet;

    private int numVertices;
    private final Timer grabTimer = Metrics.defaultRegistry().newTimer(WalkManager.class, "grab-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer dumpTimer = Metrics.defaultRegistry().newTimer(WalkManager.class, "dump-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer initTimer = Metrics.defaultRegistry().newTimer(WalkManager.class, "init-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer schedulePopulate = Metrics.defaultRegistry().newTimer(WalkManager.class, "populate-scheduler", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer restore = Metrics.defaultRegistry().newTimer(WalkManager.class, "restore", TimeUnit.SECONDS, TimeUnit.MINUTES);

    private GrabbedBucketConsumer bucketConsumer;
    private BufferedWriter log;

    public WalkManager(int numVertices, int numSources) {
        this.numVertices = numVertices;
        if (numSources > MAX_SOURCES) throw new IllegalArgumentException("Max sources: " + numSources);
        sources = new int[numSources];
        sourceWalkCounts = new int[numSources];
        sourceBitSet = new BitSet(numVertices);
        System.out.println("Initial size for walk bucket: " + initialSize);
        try {
            log = new BufferedWriter(new FileWriter(new File("walkmanager.log")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void log(String s) {
        try {
            log.write(s + "\n");
            log.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isSource(int vertexId) {
        return sourceBitSet.get(vertexId);
    }

    public int getVertexSourceIdx(int vertexId) {
        int idx = Arrays.binarySearch(sources, vertexId);
        if (idx < 0) throw new IllegalArgumentException("Vertex was not a source!");
        return idx;
    }

    public int[] getSources() {
        return sources;
    }

    public void setBucketConsumer(GrabbedBucketConsumer bucketConsumer) {
        this.bucketConsumer = bucketConsumer;
    }

    /**
     * Add a set of walks from a source. Note, you must add the walks
     * in sorted order!
     * @param vertex  source vertex
     * @param numWalks
     * @return
     */
    public synchronized int addWalkBatch(int vertex, int numWalks) {
        if (sourceSeqIdx >= sources.length)
            throw new IllegalStateException("You can have a maximum of " + sources.length + " random walk sources");

        if (sourceSeqIdx > 0) {
            if (sources[sourceSeqIdx] > vertex) {
                throw new IllegalArgumentException("You need to add sources in order!");
            }
        }

        sources[sourceSeqIdx] = vertex;
        sourceWalkCounts[sourceSeqIdx] = numWalks;
        totalWalks += numWalks;

        sourceSeqIdx++;
        return sourceSeqIdx - 1;
    }


    /**
     * Encode a walk. Note, as sourceIdx is the highest order bits, the
     * walks can be sorted by source simply by sorting the list.
     * @param sourceId index of the rousce vertex
     * @param hop true if odd, false if even
     * @param off bucket offset
     * @return
     */
    static int encode(int sourceId, boolean hop, int off) {
        assert(off < 128);
        int hopbit = (hop ? 1 : 0);
        return ((sourceId & 0xffffff) << 8) | ((off & 0x7f) << 1) | hopbit;
    }

    static int encodeV(int sourceId, boolean hop, int vertexId) {
        return encode(sourceId, hop, vertexId % bucketSize);
    }


    public static int sourceIdx(int walk) {
        return ((walk & 0xffffff00) >> 8) & 0xffffff;
    }

    public static boolean hop(int walk) {
        return ((walk & 1) != 0);
    }

    public static int off(int walk) {
        return (walk >> 1) & 0x7f;
    }


    /**
     * @param sourceId
     * @param toVertex
     * @param hop true if odd, false if even hop
     */
    public void updateWalk(int sourceId, int toVertex, boolean hop) {
        int bucket = toVertex / bucketSize;
        synchronized (bucketLocks[bucket]) {
            updateWalkUnsafe(sourceId, toVertex, hop);
        }
    }

    public void updateWalkUnsafe(int sourceId, int toVertex, boolean hop) {
        int bucket = toVertex / bucketSize;
        int w = encode(sourceId, hop, toVertex % bucketSize);
        int idx = walkIndices[bucket];
        if (idx == 0) {
            walks[bucket] = new int[initialSize];
        } else {
            if (idx == walks[bucket].length) {
                int[] newBucket = new int[walks[bucket].length * 3 / 2];
                System.arraycopy(walks[bucket], 0, newBucket, 0, walks[bucket].length);
                walks[bucket] = newBucket;
            }
        }
        walks[bucket][idx] = w;
        walkIndices[bucket]++;
    }




    public void expandCapacity(int bucket, int additional) {
        if (walks[bucket] != null) {
            int desiredLength = walks[bucket].length + additional;
            if (walks[bucket].length < desiredLength) {
                int[] newBucket = new int[desiredLength];
                System.arraycopy(walks[bucket], 0, newBucket, 0, walks[bucket].length);
                walks[bucket] = newBucket;
            }
        } else {
            walks[bucket] = new int[additional];
        }
    }

    public void initializeWalks() {
        final TimerContext _timer = initTimer.time();
        walks = new int[1 + numVertices / bucketSize][];
        bucketLocks = new Object[walks.length];
        for(int i=0; i<bucketLocks.length; i++) bucketLocks[i] = new Object();
        walkIndices = new int[walks.length];
        for(int i = 0; i < walks.length; i++) {
            walks[i] = null;
            walkIndices[i] = 0;
        }

        /* Truncate sources */
        if (sourceSeqIdx < sources.length) {
            System.out.println("Truncating...");
            int[] tmpsrcs = new int[sourceSeqIdx];
            System.arraycopy(sources, 0, tmpsrcs, 0, sourceSeqIdx);
            sources = tmpsrcs;
        }


        System.out.println("Calculate sizes. Walks length:" + walks.length);
        /* Precalculate bucket sizes for performance */
        int[] tmpsizes = new int[walks.length];
        for(int j=0; j < sourceSeqIdx; j++) {
            int source = sources[j];
            tmpsizes[source / bucketSize] += sourceWalkCounts[j];
        }


        System.out.println("Expand capacities");
        for(int b=0; b < walks.length; b++) {
            expandCapacity(b, tmpsizes[b]);
        }

        System.out.println("Allocating walks");
        for(int i=0; i < sourceSeqIdx; i++) {
            int source = sources[i];
            int count = sourceWalkCounts[i];

            int walk = encode(i, false, source % bucketSize);
            int bucket = source / bucketSize;
            int idx = walkIndices[bucket];
            for(int c=0; c<count; c++) {
                walks[bucket][idx++] = walk;
            }
            walkIndices[bucket] += count;

            if (i % 100000 == 0) System.out.println(i + " / " + sourceSeqIdx);
        }

        sourceWalkCounts = null;

        System.out.println("Set bitset...");
        // Create source-bitset
        for(int i=0; i < sourceSeqIdx; i++) {
            sourceBitSet.set(sources[i], true);
        }
        _timer.stop();
    }


    public long getTotalWalks() {
        return totalWalks;
    }

    public long getNumOfActiveWalks() {
        long s = 0;
        for(int i=0; i<walkIndices.length; i++) {
            s += walkIndices[i];
        }
        return s;
    }

    public WalkSnapshot grabSnapshot(final int fromVertex, final int toVertexInclusive) {
        final int fromBucket = fromVertex / bucketSize;
        final int toBucket = toVertexInclusive / bucketSize;
        final boolean[] snapshotInitBits = new boolean[toBucket - fromBucket + 1];
        final boolean[] processedBits = new boolean[1 + toVertexInclusive - fromVertex];
        for(int b=fromBucket; b <= toBucket; b++) {
            snapshotInitBits[b - fromBucket] = false;
        }

        /* Now create data structure for fast retrieval */
        final int[][] snapshots = new int[toVertexInclusive - fromVertex + 1][];

        /* Create the snapshot object. It creates the snapshot arrays on-demand
         *  to save memory. */
        return new WalkSnapshot() {

            @Override
            public void clear(int vertexId) {
                snapshots[vertexId - fromVertex] = null;
            }

            @Override
            public void restoreUngrabbed() {
                final TimerContext _timer = restore.time();
                // Restore such walks that were not grabbed (because the vertex
                // was not initially scheduled)
                int v = fromVertex;
                int restoreCount = 0;
                for(int[] snapshot : snapshots) {
                    if (snapshot != null && !processedBits[v - fromVertex]) {
                        for(int i=0; i<snapshot.length; i++) {
                            int w = snapshot[i];
                            updateWalk(sourceIdx(w), v, hop(w));
                            restoreCount++;
                        }
                    }
                    v++;
                }
                System.out.println("Restored " + restoreCount);
                _timer.stop();
            }

            // Note: accurate number only before snapshot is being purged
            public long numWalks() {
                long sum = 0;
                for(int b=fromBucket; b <= toBucket; b++) {
                    sum += walks[b].length;
                }
                return sum;
            }

            @Override
            public int[] getWalksAtVertex(int vertexId, boolean processed) {
                int bucketIdx = vertexId / bucketSize;
                int localBucketIdx = bucketIdx - (fromVertex / bucketSize);

                processedBits[vertexId - fromVertex] = true;

                if (snapshotInitBits[localBucketIdx]) {
                    return snapshots[vertexId - fromVertex];
                } else {
                    final TimerContext _timer = grabTimer.time();

                    int[] bucketToConsume = null;
                    int len = 0;
                    synchronized (bucketLocks[bucketIdx]) {
                        if (!snapshotInitBits[localBucketIdx]) {

                            int bucketFirstVertex = bucketSize * bucketIdx;
                            len = walkIndices[bucketIdx];

                            bucketToConsume = walks[bucketIdx];

                            if (bucketToConsume != null) {
                                walks[bucketIdx] = null;
                                walkIndices[bucketIdx] = 0;
                                final int[] snapshotSizes = new int[bucketSize];
                                final int[] snapshotIdxs = new int[bucketSize];

                                /* Calculate vertex-walks sizes */
                                for(int i=0; i < len; i++) {
                                    int w = bucketToConsume[i];
                                    snapshotSizes[off(w)]++;
                                }

                                int offt = bucketFirstVertex - fromVertex;

                                for(int i=0; i < snapshotSizes.length; i++) {
                                    if (snapshotSizes[i] > 0 && i >= -offt && i + offt < snapshots.length)
                                        snapshots[i + offt] = new int[snapshotSizes[i]];
                                }

                                for(int i=0; i < len; i++) {
                                    int w = bucketToConsume[i];
                                    int vertex = bucketFirstVertex + off(w);

                                    if (vertex >= fromVertex && vertex <= toVertexInclusive) {
                                        int snapshotOff = vertex - fromVertex;
                                        int localOff = vertex - bucketFirstVertex;
                                        snapshots[snapshotOff][snapshotIdxs[localOff]] = w;
                                        snapshotIdxs[localOff]++;
                                    } else {
                                        // add back
                                        boolean hop = hop(w);
                                        int src = sourceIdx(w);
                                        updateWalk(src, vertex, hop);
                                    }
                                }
                            }
                            snapshotInitBits[localBucketIdx] = true;
                        }
                    }
                    if (bucketConsumer != null && bucketToConsume != null && len > 0) {
                        bucketConsumer.consume(bucketIdx * bucketSize, bucketToConsume, len);
                        if (len > 1000000) {
                            log((bucketIdx * bucketSize) + " - " + ((bucketIdx+1)) * bucketSize + ", " + len);
                        }
                    }
                    _timer.stop();
                    return snapshots[vertexId - fromVertex];
                }
            }

            @Override
            public int getFirstVertex() {
                return fromVertex;
            }

            @Override
            public int getLastVertex() {
                return toVertexInclusive;
            }


        };

    }

    public static int getWalkLength(int[] w) {
        if (w == null) return 0;
        return w.length;
    }

    /** Dump to file all walks with more than 0 hop */
    public void dumpToFile(WalkSnapshot snapshot, String filename) throws IOException {
        final TimerContext _timer = dumpTimer.time();
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(filename), true)));
        for(int i=snapshot.getFirstVertex(); i <= snapshot.getLastVertex(); i++) {
            int[] ws = snapshot.getWalksAtVertex(i, false);
            if (ws != null) {
                for(int j=0; j < ws.length; j++) {
                    int w = ws[j];
                    int source = sources[sourceIdx(w)];
                    dos.writeInt(source);
                    dos.writeInt(i);
                }
            }
        }
        dos.flush();
        dos.close();
        _timer.stop();
    }

    public int getSourceVertex(int walk) {
        return sources[sourceIdx(walk)];
    }

    public void populateSchedulerWithSources(Scheduler scheduler) {
        for(int i=0; i <sources.length; i++) {
            scheduler.addTask(sources[i]);
        }
    }

    public void populateSchedulerForInterval(Scheduler scheduler, VertexInterval interval) {
        final TimerContext _timer = schedulePopulate.time();
        int fromBucket = interval.getFirstVertex() / bucketSize;
        int toBucket = interval.getLastVertex() / bucketSize;

        for(int bucketIdx=fromBucket; bucketIdx <= toBucket; bucketIdx++) {
            int vertexBase = bucketIdx * bucketSize;
            int[] bucket = walks[bucketIdx];

            if (bucket != null) {
                BitSet alreadySeen = new BitSet(bucketSize);
                int counter = 0;
                for(int j=0; j<bucket.length; j++) {
                    int off = off(bucket[j]);
                    if (!alreadySeen.get(off))  {
                        alreadySeen.set(off, true);
                        counter++;
                        scheduler.addTask(vertexBase + off);
                        if (counter == bucketSize) break;
                    }
                }
            }
        }
        _timer.stop();
    }
}
