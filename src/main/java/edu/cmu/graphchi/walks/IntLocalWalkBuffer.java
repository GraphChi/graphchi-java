package edu.cmu.graphchi.walks;

class IntLocalWalkBuffer extends LocalWalkBuffer {
    int[] walks;

    IntLocalWalkBuffer() {
        super();
        walks = new int[DEFAULT_SIZE];
    }

    public void add(int walk, int destination, boolean trackBit) {
        if (idx == walks.length) {
            int[] tmp = walks;
            walks = new int[tmp.length * 2];
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
        IntWalkManager manager = (IntWalkManager) walkManager;
        for(int i=0; i<idx; i++) {
            manager.moveWalkUnsafe(walks[i], walkBufferDests[i], trackBits[i]);
        }
        walks = null;
        walkBufferDests = null;
        trackBits = null;
    }
}

