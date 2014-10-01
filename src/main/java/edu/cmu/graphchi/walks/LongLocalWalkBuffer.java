package edu.cmu.graphchi.walks;

class LongLocalWalkBuffer extends LocalWalkBuffer {
    long[] walks;

    LongLocalWalkBuffer() {
        super();
        walks = new long[DEFAULT_SIZE];
    }

    public void add(long walk, int destination, boolean trackBit) {
        if (idx == walks.length) {
            long[] tmp = walks;
            walks = new long[tmp.length * 2];
            System.arraycopy(tmp, 0, walks, 0, tmp.length);

            expandArrays();
        }
        walkBufferDests[idx] = destination;
        walks[idx] = walk;
        trackBits[idx] = trackBit;
        idx++;
    }

    @Override
    public void purge(WalkManager walkManager) {
        LongWalkManager manager = (LongWalkManager) walkManager;
        for(int i=0; i<idx; i++) {
            manager.moveWalkUnsafe(walks[i], walkBufferDests[i], trackBits[i]);
        }
        walks = null;
        walkBufferDests = null;
        trackBits = null;
    }
}

