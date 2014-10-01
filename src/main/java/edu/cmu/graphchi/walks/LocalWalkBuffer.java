package edu.cmu.graphchi.walks;

import java.util.Random;

abstract class LocalWalkBuffer {
    int[] walkBufferDests;
    boolean[] trackBits;

    int idx = 0;
    int DEFAULT_SIZE = 65536;
    Random random;

    LocalWalkBuffer() {
        walkBufferDests = new int[DEFAULT_SIZE];
        trackBits = new boolean[DEFAULT_SIZE];
        random = new Random();
    }

    protected void expandArrays() {
        int[] tmp = walkBufferDests;
        walkBufferDests = new int[tmp.length * 2];
        System.arraycopy(tmp, 0, walkBufferDests, 0, tmp.length);

        boolean[] tmpB = trackBits;
        trackBits = new boolean[tmpB.length * 2];
        System.arraycopy(tmpB, 0, trackBits, 0, tmpB.length);
    }

    public abstract void purge(WalkManager walkManager);
}
