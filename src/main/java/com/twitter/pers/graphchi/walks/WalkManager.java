package com.twitter.pers.graphchi.walks;

import java.io.*;
import java.util.ArrayList;

/**
 * Manager for random walks
 * @author Aapo Kyrola
 */
public class WalkManager {

    private final static int bucketSize = 1024; // Store walks into buckets for faster retrieval
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
                int[] newBucket = new int[walks[bucket].length * 3 / 2];
                System.arraycopy(walks[bucket], 0, newBucket, 0, walks[bucket].length);
                walks[bucket] = newBucket;
            }
            walks[bucket][idx] = encode(sourceId, hop, toVertex % bucketSize);
            walkIndices[bucket]++;
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
        final int fromBucket = fromVertex / bucketSize;
        final int toBucket = toVertexInclusive / bucketSize;

        /* Replace the buckets in question with empty buckets */
        final ArrayList<int[]> tmpBuckets = new ArrayList<int[]>(toBucket - fromBucket + 1);
        final int[] tmpBucketLengths = new int[toBucket - fromBucket + 1];
        for(int b=fromBucket; b <= toBucket; b++) {
            int[] bucket = walks[b];
            int len = walkIndices[b];
            tmpBuckets.add(bucket);
            tmpBucketLengths[b - fromBucket] = len;
            walks[b] = new int[initialSize];
            walkIndices[b] = 0;

            // Add walks back that do not match the range -- todo : force aligned!
            if (b == fromBucket || b == toBucket) {
                int bucketFirstVertex = b * bucketSize;
                for(int i=0; i < len; i++) {
                    int vid = off(bucket[i]) + bucketFirstVertex;
                    if (vid < fromVertex || vid > toVertexInclusive)
                        updateWalk(sourceIdx(bucket[i]), vid, hop(bucket[i]));
                }
            }
        }

        /* Create the snapshot object.
         * This is a memory-efficient that scans the bucket twice */
        return new WalkSnapshot() {

            @Override
            public int[] getWalksAtVertex(int vertexId) {
                int bucketIdx = vertexId / bucketSize - fromBucket;
                int[] bucket = tmpBuckets.get(bucketIdx);
                // Count how many
                int n = 0;
                int myOff = vertexId % bucketSize;
                int bucketLength = tmpBucketLengths[bucketIdx];
                for(int i=0; i < bucketLength; i++) {
                    if (off(bucket[i]) == myOff) {
                        n++;
                    }
                }

                if (n == 0) return null;

                int[] walks = new int[n];
                int idx = 0;
                for(int i=0; i < bucketLength; i++) {
                    if (off(bucket[i]) == myOff) {
                        walks[idx++] = bucket[i];
                    }
                }
                return walks;
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
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(filename), true)));
        for(int i=snapshot.getFirstVertex(); i <= snapshot.getLastVertex(); i++) {
            int[] ws = snapshot.getWalksAtVertex(i);
            if (ws != null) {
                for(int j=0; j < ws.length; j++) {
                    int w = ws[j];
                    if (hop(w) > 0) {
                        int source = sources[sourceIdx(w)];
                        dos.writeInt(source);
                        dos.writeInt(i);
                    }
                }
            }
        }
        dos.flush();
        dos.close();
    }

    public int getSourceVertex(int srcIdx) {
        return sources[srcIdx];
    }
}
