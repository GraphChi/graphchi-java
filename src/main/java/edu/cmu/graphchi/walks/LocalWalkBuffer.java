package edu.cmu.graphchi.walks;

import java.util.ArrayList;
import java.util.Random;

class LocalWalkBuffer {
    int[] walkBufferDests;
    int[] walkSourcesAndHops;
    Random random = new Random();

    ArrayList<int[]> dests = new ArrayList<int[]>();
    ArrayList<int[]> hops = new ArrayList<int[]>();




    int idx = 0;
    LocalWalkBuffer() {
        walkBufferDests = new int[65536];
        walkSourcesAndHops = new int[65536];
    }

    public void add(int src, int dst, boolean hop) {
        if (idx == walkSourcesAndHops.length) {
            dests.add(walkBufferDests);
            hops.add(walkSourcesAndHops);
            walkBufferDests = new int[1000000];
            walkSourcesAndHops = new int[1000000];
            idx = 0;
        }
        walkBufferDests[idx] = dst;
        walkSourcesAndHops[idx] = (hop ? -1 : 1) * (1 + src); // Note +1 so zero will be handled correctly
        idx++;
    }

    public void purge(WalkManager walkManager) {
        dests.add(walkBufferDests);
        hops.add(walkSourcesAndHops);

        for(int k=0; k < hops.size(); k++) {
            int[] d = dests.get(k);
            int[] h = hops.get(k);
            int len = (k == hops.size() - 1 ? idx : d.length);
            for(int i=0; i < len; i++) {
                int dst = d[i];
                int src = h[i];
                boolean hop = src < 0;
                if (src < 0) src = -src;
                src = src - 1;  // Note, -1
                walkManager.updateWalkUnsafe(src, dst, hop);
            }
        }
        hops = null;
        dests = null;
        walkSourcesAndHops = null;
        walkBufferDests = null;
    }
}
