package edu.cmu.graphchi.walks;

import java.util.Random;

class LocalWalkBuffer {
    int[] walkBufferDests;
    int[] walkSourcesAndHops;
    Random random = new Random();

    int idx = 0;
    LocalWalkBuffer() {
        walkBufferDests = new int[65536];
        walkSourcesAndHops = new int[65536];
    }

    public void add(int src, int dst, boolean hop) {
        if (idx == walkSourcesAndHops.length) {
            int[] tmp = walkSourcesAndHops;
            walkSourcesAndHops = new int[tmp.length * 2];
            System.arraycopy(tmp, 0, walkSourcesAndHops, 0, tmp.length);

            tmp = walkBufferDests;
            walkBufferDests = new int[tmp.length * 2];
            System.arraycopy(tmp, 0, walkBufferDests, 0, tmp.length);
        }
        walkBufferDests[idx] = dst;
        walkSourcesAndHops[idx] = (hop ? -1 : 1) * (1 + src); // Note +1 so zero will be handled correctly
        idx++;
    }

    public void purge(WalkManager walkManager) {
        for(int i=0; i<idx; i++) {
            int dst = walkBufferDests[i];
            int src = walkSourcesAndHops[i];
            boolean hop = src < 0;
            if (src < 0) src = -src;
            src = src - 1;  // Note, -1
            walkManager.updateWalkUnsafe(src, dst, hop);
        }
        walkSourcesAndHops = null;
        walkBufferDests = null;
    }
}
