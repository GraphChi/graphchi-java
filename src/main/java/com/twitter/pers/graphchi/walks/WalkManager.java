package com.twitter.pers.graphchi.walks;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import edu.cmu.graphchi.Scheduler;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Manager for random walks
 * @author Aapo Kyrola
 */
public class WalkManager {

    private final static int bucketSize = 1024; // Store walks into buckets for faster retrieval
    private final static int initialSize = Integer.parseInt(System.getProperty("walkmanager.initial_size", "256"));

    private int sourceSeqIdx  = 0;
    private int[] sources = new int[32678];
    private int[] sourceWalkCounts = new int[32678];
    private int totalWalks = 0;

    private int[][] walks;
    private int[] walkIndices;

    private int numVertices;
    private final Timer grabTimer = Metrics.newTimer(WalkManager.class, "grab-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer dumpTimer = Metrics.newTimer(WalkManager.class, "dump-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer initTimer = Metrics.newTimer(WalkManager.class, "init-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);


    public WalkManager(int numVertices) {
        this.numVertices = numVertices;
        System.out.println("Initial size for walk bucket: " + initialSize);
    }

    public synchronized int addWalkBatch(int vertex, int numWalks) {
        if (sourceSeqIdx >= sources.length)
            throw new IllegalStateException("You can have a maximum of 32678 random walk sources");

        sources[sourceSeqIdx] = vertex;
        sourceWalkCounts[sourceSeqIdx] = numWalks;
        totalWalks += numWalks;

        sourceSeqIdx++;
        return sourceSeqIdx - 1;
    }


    public int encode(int sourceId, int hop, int off) {
        return sourceId | ((hop << 16) & 0x000f0000) | ((off << 20) & 0xfff00000);
    }

    public int sourceIdx(int walk) {
        return walk & 0x0000ffff;
    }

    public int hop(int walk) {
        return (walk & 0x000f0000) >> 16;
    }

    public int off(int walk) {
        return (walk & 0xfff00000) >> 20;
    }


    public void updateWalk(int sourceId, int toVertex, int hop) {
        int bucket = toVertex / bucketSize;
        assert(hop < 16);

        synchronized (walks[bucket]) {
            int idx = walkIndices[bucket];
            if (idx == walks[bucket].length) {
                int[] newBucket = new int[walks[bucket].length * 3 / 2];
                System.arraycopy(walks[bucket], 0, newBucket, 0, walks[bucket].length);
                walks[bucket] = newBucket;
            }
            walks[bucket][idx] = encode(sourceId, hop, toVertex % bucketSize);
            walkIndices[bucket]++;
        }
    }

    public void expandCapacity(int bucket, int additional) {
        int desiredLength = walks[bucket].length + additional;

        if (walks[bucket].length < desiredLength) {
            int[] newBucket = new int[desiredLength];
            System.arraycopy(walks[bucket], 0, newBucket, 0, walks[bucket].length);
            walks[bucket] = newBucket;
        }
    }

    public void initializeWalks() {
        final TimerContext _timer = initTimer.time();
        walks = new int[1 + numVertices / bucketSize][];
        walkIndices = new int[walks.length];
        for(int i = 0; i < walks.length; i++) {
            walks[i] = new int[initialSize];
            walkIndices[i] = 0;
        }

        /* Precalculate bucket sizes for performance */
        int[] tmpsizes = new int[walks.length];
        for(int j=0; j < sourceSeqIdx; j++) {
            int source = sources[j];
            tmpsizes[source / bucketSize] += sourceWalkCounts[j];
        }

        for(int b=0; b < walks.length; b++) {
            expandCapacity(b, tmpsizes[b]);
        }

        // TODO: allocate to match the required size (should be easy)
        for(int i=0; i < sourceSeqIdx; i++) {
            int source = sources[i];
            int count = sourceWalkCounts[i];
            for(int c=0; c<count; c++) updateWalk(i, source, 0);
        }
        _timer.stop();
    }


    public int getTotalWalks() {
        return totalWalks;
    }

    public WalkSnapshot grabSnapshot(final int fromVertex, final int toVertexInclusive) {
        final TimerContext _timer = grabTimer.time();
        int fromBucket = fromVertex / bucketSize;
        int toBucket = toVertexInclusive / bucketSize;

        /* Replace the buckets in question with empty buckets */
        ArrayList<int[]> tmpBuckets = new ArrayList<int[]>(toBucket - fromBucket + 1);
        int[] tmpBucketLengths = new int[toBucket - fromBucket + 1];
        for(int b=fromBucket; b <= toBucket; b++) {
            tmpBuckets.add(walks[b]);
            tmpBucketLengths[b - fromBucket] = walkIndices[b];
            walks[b] = new int[initialSize];
            walkIndices[b] = 0;
        }

        /* Now create data structure for fast retrieval */
        final int[][] snapshots = new int[toVertexInclusive - fromVertex + 1][];
        final int[] snapshotIdxs = new int[snapshots.length];

        for(int i=0; i < snapshots.length; i++) {
            snapshots[i] = null;
            snapshotIdxs[i] = 0;
        }
        /* Add walks to snapshot arrays -- TODO: parallelize */
        for(int b=0; b < tmpBuckets.size(); b++) {
            int bucketFirstVertex = bucketSize * (fromBucket + b);
            int[] arr = tmpBuckets.get(b);
            int len = tmpBucketLengths[b];

            final int[] snapshotSizes = new int[bucketSize];

            /* Calculate vertex-walks sizes */
            for(int i=0; i < len; i++) {
                int w = arr[i];
                snapshotSizes[off(w)]++;
            }

            int offt = bucketFirstVertex - fromVertex;

            /* Precalculate the array sizes. offt is the
               offset of the bucket's first vertex from the first
               vertex of the snapshot
             */

            for(int i=0; i < snapshotSizes.length; i++) {
                if (snapshotSizes[i] > 0 && i >= -offt && i + offt < snapshots.length)
                    snapshots[i + offt] = new int[snapshotSizes[i]];
            }

            for(int i=0; i < len; i++) {
                int w = arr[i];
                int hop = hop(w);
                int vertex = bucketFirstVertex + off(w);
                int src = sourceIdx(w);

                if (vertex >= fromVertex && vertex <= toVertexInclusive) {
                    int snapshotOff = vertex - fromVertex;
                    if (snapshots[snapshotOff] == null)
                        throw new IllegalStateException();

                    if (snapshotIdxs[snapshotOff] >= snapshots[snapshotOff].length) {
                        throw new RuntimeException("Not possible!");
                        /*   Duplicate array
                        int[] tmp = new int[snapshots[snapshotOff].length * 2];
                        System.arraycopy(snapshots[snapshotOff], 0, tmp, 0, snapshots[snapshotOff].length);
                        snapshots[snapshotOff] = tmp;   */
                    }
                    snapshots[snapshotOff][snapshotIdxs[snapshotOff]] = w;
                    snapshotIdxs[snapshotOff]++;
                } else {
                    // add back
                    updateWalk(src, vertex, hop);
                }
            }
            tmpBuckets.set(b, null); // Save memory
        }

        _timer.stop();

        /* Create the snapshot object */
        return new WalkSnapshot() {
            @Override
            public int[] getWalksAtVertex(int vertexId) {
                return snapshots[vertexId - fromVertex];
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
            int[] ws = snapshot.getWalksAtVertex(i);
            if (ws != null) {
                for(int j=0; j < ws.length; j++) {
                    int w = ws[j];
                    if (hop(w) > 2) { // NOTE! To save disk space
                        int source = sources[sourceIdx(w)];
                        dos.writeInt(source);
                        dos.writeInt(i);
                    }
                }
            }
        }
        dos.flush();
        dos.close();
        _timer.stop();
    }

    public int getSourceVertex(int srcIdx) {
        return sources[srcIdx];
    }

    public void populateSchedulerWithSources(Scheduler scheduler) {
        for(int i=0; i <sources.length; i++) {
            scheduler.addTask(sources[i]);
        }
    }
}
