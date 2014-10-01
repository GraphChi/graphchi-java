package edu.cmu.graphchi.walks;

import com.yammer.metrics.core.TimerContext;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.Scheduler;
import edu.cmu.graphchi.engine.VertexInterval;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Manager for random walks
 * * Done partially during authors internship at Twitter, Fall 2012.
 * @author Aapo Kyrola,  akyrola@cs.cmu.edu
 */
public class LongWalkManager extends WalkManager {

    protected long[][] walks;

    private static final Logger logger = ChiLogger.getLogger("long-walk-manager");

    public LongWalkManager(int numVertices, int numSources) {
        super(numVertices, numSources);
    }

    @Override
    protected void setSourceAndBucketBits() {
        MAX_SOURCES = 16777216;  // 24 bits for the source id
        bucketSize = 128;   // 7 bits for the bucket size
    }

    /**
     * Encode a walk. Note, as sourceIdx is the highest order bits, the
     * walks can be sorted by source simply by sorting the list.
     * @param sourceId index of the rousce vertex
     * @param hop true if odd, false if even
     * @param off bucket offset
     * @return
     */
    protected long encode(int sourceId, boolean hop, int off) {
        assert(off < 128);
        int hopbit = (hop ? 1 : 0);
        return ((sourceId & 0xffffff) << 8) | ((off & 0x7f) << 1) | hopbit;
    }

    protected long encodeV(int sourceId, boolean hop, int vertexId) {
        return encode(sourceId, hop, vertexId % bucketSize);
    }

    protected long encodeNewWalk(int sourceId, int sourceVertex, boolean hop) {
        return encode(sourceId, hop, sourceVertex % bucketSize);
    }

    public int sourceIdx(long walk) {
        return (int) ((walk & 0xffffff00) >> 8) & 0xffffff;
    }

    public boolean trackBit(long walk) {
        return ((walk & 1) != 0);
    }

    public int off(long walk) {
        return (int) (walk >> 1) & 0x7f;
    }

    /**
     * Resets the bucket offset to reflect the new destination vertex, and also resets the track
     * bit, according to the parameters.  Note that those are the _only_ things re-encoded by this
     * method, as those are the only things this method has access to; if other parts of the walk
     * need to be changed, that must be taken care of in the WalkUpdateFunction _before_ forwarding
     * the walk.
     */
    protected long reencodeWalk(long walk, int toVertex, boolean trackBit) {
        int bucket = toVertex / bucketSize;
        return encode(sourceIdx(walk), trackBit, toVertex % bucketSize);
    }

    /**
     * @param sourceId
     * @param toVertex
     * @param trackBit true if odd, false if even hop
     */
    public void moveWalk(long walk, int toVertex, boolean trackBit) {
        int bucket = toVertex / bucketSize;
        synchronized (bucketLocks[bucket]) {
            moveWalkUnsafe(walk, toVertex, trackBit);
        }
    }

    public void moveWalkUnsafe(long walk, int toVertex, boolean trackBit) {
        // Reincode the walk to reflect the movement
        walk = reencodeWalk(walk, toVertex, trackBit);

        // Move the walk to the new bucket for processing
        int bucket = toVertex / bucketSize;
        int idx = walkIndices[bucket];
        if (idx == 0) {
            walks[bucket] = new long[initialSize];
        } else {
            if (idx == walks[bucket].length) {
                long[] newBucket = new long[walks[bucket].length * 3 / 2];
                System.arraycopy(walks[bucket], 0, newBucket, 0, walks[bucket].length);
                walks[bucket] = newBucket;
            }
        }
        walks[bucket][idx] = walk;
        walkIndices[bucket]++;
    }

    @Override
    protected void expandCapacity(int bucket, int additional) {
        if (walks[bucket] != null) {
            int desiredLength = walks[bucket].length + additional;
            if (walks[bucket].length < desiredLength) {
                long[] newBucket = new long[desiredLength];
                System.arraycopy(walks[bucket], 0, newBucket, 0, walks[bucket].length);
                walks[bucket] = newBucket;
            }
        } else {
            walks[bucket] = new long[additional];
        }
    }

    @Override
    public void initializeWalks() {
        walks = new long[1 + numVertices / bucketSize][];
        bucketLocks = new Object[walks.length];
        for(int i=0; i<bucketLocks.length; i++) bucketLocks[i] = new Object();
        walkIndices = new int[walks.length];
        for(int i = 0; i < walks.length; i++) {
            walks[i] = null;
            walkIndices[i] = 0;
        }

        /* Truncate sources */
        if (sourceSeqIdx < sources.length) {
            logger.info("Truncating...");
            int[] tmpsrcs = new int[sourceSeqIdx];
            System.arraycopy(sources, 0, tmpsrcs, 0, sourceSeqIdx);
            sources = tmpsrcs;
        }


        logger.info("Calculate sizes. Walks length:" + walks.length);
        /* Precalculate bucket sizes for performance */
        int[] tmpsizes = new int[walks.length];
        for(int j=0; j < sourceSeqIdx; j++) {
            int source = sources[j];
            tmpsizes[source / bucketSize] += sourceWalkCounts[j];
        }


        logger.info("Expand capacities");
        for(int b=0; b < walks.length; b++) {
            expandCapacity(b, tmpsizes[b]);
        }

        logger.info("Allocating walks");
        for(int i=0; i < sourceSeqIdx; i++) {
            int source = sources[i];
            int count = sourceWalkCounts[i];

            //long walk = encodeNewWalk(i, source, false);
            int bucket = source / bucketSize;
            int idx = walkIndices[bucket];
            for(int c=0; c<count; c++) {
                // This is a little slower than just encoding the walk once for each source, but
                // some applications need to keep track of a walk id.  Calling encodeNewWalk every
                // time allows for this.
                walks[bucket][idx++] = encodeNewWalk(i, source, false);
            }
            walkIndices[bucket] += count;

            if (i % 100000 == 0) logger.info(i + " / " + sourceSeqIdx);
        }

        sourceWalkCounts = null;

        logger.info("Set bitset...");
        // Create source-bitset
        for(int i=0; i < sourceSeqIdx; i++) {
            sourceBitSet.set(sources[i], true);
        }
    }

    @Override
    public WalkSnapshot grabSnapshot(final int fromVertex, final int toVertexInclusive) {
        final int fromBucket = fromVertex / bucketSize;
        final int toBucket = toVertexInclusive / bucketSize;
        final boolean[] snapshotInitBits = new boolean[toBucket - fromBucket + 1];
        final boolean[] processedBits = new boolean[1 + toVertexInclusive - fromVertex];
        for(int b=fromBucket; b <= toBucket; b++) {
            snapshotInitBits[b - fromBucket] = false;
        }

        /* Now create data structure for fast retrieval */
        final long[][] snapshots = new long[toVertexInclusive - fromVertex + 1][];

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
                for(long[] snapshot : snapshots) {
                    if (snapshot != null && !processedBits[v - fromVertex]) {
                        for(int i=0; i<snapshot.length; i++) {
                            long w = snapshot[i];
                            moveWalk(w, v, trackBit(w));
                            restoreCount++;
                        }
                    }
                    v++;
                }
                logger.info("Restored " + restoreCount);
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
            public WalkArray getWalksAtVertex(int vertexId, boolean processed) {
                int bucketIdx = vertexId / bucketSize;
                int localBucketIdx = bucketIdx - (fromVertex / bucketSize);

                processedBits[vertexId - fromVertex] = true;

                if (snapshotInitBits[localBucketIdx]) {
                    long[] array = snapshots[vertexId - fromVertex];
                    if (array == null) {
                        return null;
                    } else {
                        return new LongWalkArray(snapshots[vertexId - fromVertex]);
                    }
                } else {
                    final TimerContext _timer = grabTimer.time();

                    long[] bucketToConsume = null;
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
                                    long w = bucketToConsume[i];
                                    snapshotSizes[off(w)]++;
                                }

                                int offt = bucketFirstVertex - fromVertex;

                                for(int i=0; i < snapshotSizes.length; i++) {
                                    if (snapshotSizes[i] > 0 && i >= -offt && i + offt < snapshots.length)
                                        snapshots[i + offt] = new long[snapshotSizes[i]];
                                }

                                for(int i=0; i < len; i++) {
                                    long w = bucketToConsume[i];
                                    int vertex = bucketFirstVertex + off(w);

                                    if (vertex >= fromVertex && vertex <= toVertexInclusive) {
                                        int snapshotOff = vertex - fromVertex;
                                        int localOff = vertex - bucketFirstVertex;
                                        snapshots[snapshotOff][snapshotIdxs[localOff]] = w;
                                        snapshotIdxs[localOff]++;
                                    } else {
                                        // add back
                                        moveWalk(w, vertex, trackBit(w));
                                    }
                                }
                            }
                            snapshotInitBits[localBucketIdx] = true;
                        }
                    }
                    if (bucketConsumer != null && bucketToConsume != null && len > 0) {
                        bucketConsumer.consume(bucketIdx * bucketSize, new LongWalkArray(bucketToConsume), len);
                        if (len > 1000000) {
                            log((bucketIdx * bucketSize) + " - " + ((bucketIdx+1)) * bucketSize + ", " + len);
                        }
                    }
                    _timer.stop();
                    long[] array = snapshots[vertexId - fromVertex];
                    if (array == null) {
                        return null;
                    } else {
                        return new LongWalkArray(snapshots[vertexId - fromVertex]);
                    }
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

    /** Dump to file all walks with more than 0 hop */
    @Override
    public void dumpToFile(WalkSnapshot snapshot, String filename) throws IOException {
        final TimerContext _timer = dumpTimer.time();
        synchronized (filename.intern()) {
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(filename), true)));
            for(int i=snapshot.getFirstVertex(); i <= snapshot.getLastVertex(); i++) {
                long[] ws = ((LongWalkArray)snapshot.getWalksAtVertex(i, false)).getArray();
                if (ws != null) {
                    for(int j=0; j < ws.length; j++) {
                        long w = ws[j];
                        int source = sources[sourceIdx(w)];
                        dos.writeLong(source);
                        dos.writeInt(i);
                    }
                }
            }
            dos.flush();
            dos.close();
        }
        _timer.stop();
    }

    public int getSourceVertex(long walk) {
        return sources[sourceIdx(walk)];
    }

    @Override
    public void populateSchedulerForInterval(Scheduler scheduler, VertexInterval interval) {
        final TimerContext _timer = schedulePopulate.time();
        int fromBucket = interval.getFirstVertex() / bucketSize;
        int toBucket = interval.getLastVertex() / bucketSize;

        for(int bucketIdx=fromBucket; bucketIdx <= toBucket; bucketIdx++) {
            int vertexBase = bucketIdx * bucketSize;
            long[] bucket = walks[bucketIdx];

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
