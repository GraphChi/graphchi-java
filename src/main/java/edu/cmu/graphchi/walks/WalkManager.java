package edu.cmu.graphchi.walks;

import java.util.ArrayList;

/**
 * Manager for random walks
 */
public class WalkManager {

    private final static int bucketSize = 1024; // Store walks into buckets for waster retrieval
    private final static int initialSize = 32;

    private int sourceSeqIdx  = 0;
    private int[] sources = new int[32678];
    private int[] sourceWalkCounts = new int[32678];
    private int totalWalks = 0;

    private int[][] walks;
    private int[] walkIndices;

    private int numVertices;

    public WalkManager(int numVertices) {
        this.numVertices = numVertices;
    }

    public synchronized int addWalkBatch(int vertex, int numWalks) {
        if (sourceSeqIdx >= sources.length)
            throw new IllegalStateException("You can have a maximum of 32678 random walk sources");

        sources[sourceSeqIdx] = vertex;
        sourceWalkCounts[sourceSeqIdx] = numWalks;
        totalWalks += numWalks;
        if ((long)totalWalks + numWalks > 2e9) { // Arbitrary
            throw new IllegalStateException("Too many walks!");
        }
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
                int[] newBucket = new int[walks[bucket].length * 2];
                System.arraycopy(walks[bucket], 0, newBucket, 0, walks[bucket].length);
                walks[bucket] = newBucket;
            }
            walks[bucket][idx] = encode(sourceId, hop, toVertex % bucketSize);
            walkIndices[bucket] = 0;
        }
    }

    public void initializeWalks() {
        walks = new int[1 + numVertices / bucketSize][];
        walkIndices = new int[walks.length];
        for(int i = 0; i < walks.length; i++) {
            walks[i] = new int[initialSize];
            walkIndices[i] = 0;
        }

        for(int i=0; i < sourceSeqIdx; i++) {
            int source = sources[i];
            int count = sourceWalkCounts[i];
            for(int c=0; c<count; c++) updateWalk(i, source, 0);
        }
    }


    public int getTotalWalks() {
        return totalWalks;
    }

    public WalkSnapshot grabSnapshot(final int fromVertex, final int toVertexInclusive) {
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
        final int[][] snapshots = new int[toVertexInclusive - fromBucket + 1][];
        final int[] snapshotIdxs = new int[snapshots.length];
        for(int i=0; i < snapshots.length; i++) {
            snapshots[i] = new int[32];
            snapshotIdxs[i] = 0;
        }
        /* Add walks to snapshot arrays -- TODO: parallelize */
        for(int b=0; b < tmpBuckets.size(); b++) {
            int bucketFirstVertex = bucketSize * (fromBucket + b);
            int[] arr = tmpBuckets.get(b);
            int len = tmpBucketLengths[b];
            for(int i=0; i < len; i++) {
                int w = arr[i];
                int hop = hop(w);
                int vertex = bucketFirstVertex + off(w);
                int src = sourceIdx(w);

                if (vertex >= fromVertex && vertex <= toVertexInclusive) {
                    int snapshotOff = vertex - fromVertex;
                    if (snapshotIdxs[snapshotOff] >= snapshots[snapshotOff].length) {
                        /* Duplicate array */
                        int[] tmp = new int[snapshots[snapshotOff].length * 2];
                        System.arraycopy(snapshots[snapshotOff], 0, tmp, 0, snapshots[snapshotOff].length);
                        snapshots[snapshotOff] = tmp;
                    }
                    snapshots[snapshotOff][snapshotIdxs[snapshotOff]] = w;
                    snapshotIdxs[snapshotOff]++;
                } else {
                    // add back
                    updateWalk(src, vertex, hop);
                }
            }
            tmpBuckets.set(b, null); // Save memory

            /* Snap to correct size */
            for(int s=0; s < snapshots.length; b++) {
                if (snapshots[s].length != snapshotIdxs[s]) {
                    int[] tmp = new int[snapshotIdxs[s]];
                    System.arraycopy(snapshots[s], 0, tmp, 0, snapshots[s].length);
                    snapshots[s] = tmp;
                }
            }
        }



        /* Create the snapshot object */
        return new WalkSnapshot() {
            @Override
            public int[] getWalksAtVertex(int vertexId) {
                return snapshots[vertexId - fromVertex];
            }
        };
    }

}
