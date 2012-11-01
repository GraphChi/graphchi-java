package com.twitter.pers.graphchi.walks;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import edu.cmu.graphchi.Scheduler;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Manager for random walks. This version has an unique id for each walk and thus
 * uses 64-bits for each walk
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
public class WalkManagerForPaths {

    private final static int bucketSize = 1024; // Store walks into buckets for faster retrieval
    private final static int initialSize = Integer.parseInt(System.getProperty("walkmanager.initial_size", "256"));

    private ArrayList<Integer> sources = new ArrayList<Integer>(32678);
    private ArrayList<Integer> sourceWalkCounts = new ArrayList<Integer>(32678);
    private int totalWalks = 0;

    private long[][] walks;
    private int[] walkIndices;

    private int numVertices;
    private final Timer grabTimer = Metrics.defaultRegistry().newTimer(WalkManagerForPaths.class, "grab-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer dumpTimer = Metrics.defaultRegistry().newTimer(WalkManagerForPaths.class, "dump-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer initTimer = Metrics.defaultRegistry().newTimer(WalkManagerForPaths.class, "init-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);


    public WalkManagerForPaths(int numVertices) {
        this.numVertices = numVertices;
        System.out.println("Initial size for walk bucket: " + initialSize);
    }

    public synchronized void addWalkBatch(int vertex, int numWalks) {
        sources.add(vertex);
        sourceWalkCounts.add(numWalks);
        totalWalks += numWalks;

    }


    // Note: there are some extra bits to be used here
    public long encode(int id, int hop, int off) {
        return ((long)id) << 32 | (((long)hop << 16) & 0x000f0000l) | ((off << 20) & 0xfff00000l);
    }


    public int hop(long walk) {
        return (int) ((walk & 0x000f0000) >> 16);
    }

    public int off(long walk) {
        return (int) ((walk & 0xfff00000l) >> 20);
    }

    public int walkId(long walk) {
        return (int) (walk >> 32);
    }


    public void updateWalk(int id, int toVertex, int hop) {
        int bucket = toVertex / bucketSize;
        assert(hop < 16);


        synchronized (walks[bucket]) {
            int idx = walkIndices[bucket];
            if (idx == walks[bucket].length) {
                long[] newBucket = new long[walks[bucket].length * 3 / 2];
                System.arraycopy(walks[bucket], 0, newBucket, 0, walks[bucket].length);
                walks[bucket] = newBucket;
            }
            walks[bucket][idx] = encode(id, hop, toVertex % bucketSize);
            walkIndices[bucket]++;
        }
    }

    public void expandCapacity(int bucket, int additional) {
        int desiredLength = walks[bucket].length + additional;

        if (walks[bucket].length < desiredLength) {
            long[] newBucket = new long[desiredLength];
            System.arraycopy(walks[bucket], 0, newBucket, 0, walks[bucket].length);
            walks[bucket] = newBucket;
        }
    }

    public void initializeWalks() {
        final TimerContext _timer = initTimer.time();
        walks = new long[1 + numVertices / bucketSize][];
        walkIndices = new int[walks.length];
        for(int i = 0; i < walks.length; i++) {
            walks[i] = new long[initialSize];
            walkIndices[i] = 0;
        }

        /* Precalculate bucket sizes for performance */
        int[] tmpsizes = new int[walks.length];
        for(int j=0; j < sources.size(); j++) {
            int source = sources.get(j);
            tmpsizes[source / bucketSize] += sourceWalkCounts.get(j);
        }

        for(int b=0; b < walks.length; b++) {
            expandCapacity(b, tmpsizes[b]);
        }

        int walkId = 0;
        for(int i=0; i < sources.size(); i++) {
            int source = sources.get(i);
            int count = sourceWalkCounts.get(i);
            for(int c=0; c<count; c++) updateWalk(walkId++, source, 0);
        }
        _timer.stop();
    }


    public int getTotalWalks() {
        return totalWalks;
    }

    public WalkSnapshotForPaths grabSnapshot(final int fromVertex, final int toVertexInclusive) {
        final TimerContext _timer = grabTimer.time();
        int fromBucket = fromVertex / bucketSize;
        int toBucket = toVertexInclusive / bucketSize;

        /* Replace the buckets in question with empty buckets */
        ArrayList<long[]> tmpBuckets = new ArrayList<long[]>(toBucket - fromBucket + 1);
        int[] tmpBucketLengths = new int[toBucket - fromBucket + 1];
        for(int b=fromBucket; b <= toBucket; b++) {
            tmpBuckets.add(walks[b]);
            tmpBucketLengths[b - fromBucket] = walkIndices[b];
            walks[b] = new long[initialSize];
            walkIndices[b] = 0;
        }

        /* Now create data structure for fast retrieval */
        final long[][] snapshots = new long[toVertexInclusive - fromVertex + 1][];
        final int[] snapshotIdxs = new int[snapshots.length];

        for(int i=0; i < snapshots.length; i++) {
            snapshots[i] = null;
            snapshotIdxs[i] = 0;
        }
        /* Add walks to snapshot arrays -- TODO: parallelize */
        for(int b=0; b < tmpBuckets.size(); b++) {
            int bucketFirstVertex = bucketSize * (fromBucket + b);
            long[] arr = tmpBuckets.get(b);
            int len = tmpBucketLengths[b];

            final int[] snapshotSizes = new int[bucketSize];

            /* Calculate vertex-walks sizes */
            for(int i=0; i < len; i++) {
                long w = arr[i];
                snapshotSizes[off(w)]++;
            }

            int offt = bucketFirstVertex - fromVertex;

            /* Precalculate the array sizes. offt is the
               offset of the bucket's first vertex from the first
               vertex of the snapshot
             */

            for(int i=0; i < snapshotSizes.length; i++) {
                if (snapshotSizes[i] > 0 && i >= -offt && i + offt < snapshots.length)
                    snapshots[i + offt] = new long[snapshotSizes[i]];
            }

            for(int i=0; i < len; i++) {
                long w = arr[i];
                int hop = hop(w);
                int id = walkId(w);
                int vertex = bucketFirstVertex + off(w);

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
                    updateWalk(id, vertex, hop);
                }
            }
            tmpBuckets.set(b, null); // Save memory
        }

        _timer.stop();

        /* Create the snapshot object */
        return new WalkSnapshotForPaths() {
            @Override
            public long[] getWalksAtVertex(int vertexId) {
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



    /** Dump to file all walks with more than 0 hop */
    public void dumpToFile(WalkSnapshotForPaths snapshot, String filename) throws IOException {
        final TimerContext _timer = dumpTimer.time();
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(filename), true)));
        for(int i=snapshot.getFirstVertex(); i <= snapshot.getLastVertex(); i++) {
            long[] ws = snapshot.getWalksAtVertex(i);
            if (ws != null) {
                for(int j=0; j < ws.length; j++) {
                    long w = ws[j];
                    /* walk-id: int, hop: short, vertex: int */
                    dos.writeInt(walkId(w));
                    dos.writeShort(hop(w));
                    dos.writeInt(i);
                }
            }
        }
        dos.flush();
        dos.close();
        _timer.stop();
    }


    public void populateSchedulerWithSources(Scheduler scheduler) {
        for(int i=0; i < sources.size(); i++) {
            scheduler.addTask(sources.get(i));
        }
    }
}
