package edu.cmu.graphchi.walks;

public class LongWalkArray extends WalkArray {
    private long[] array;

    public LongWalkArray(long[] array) {
        this.array = array;
    }

    @Override
    public int size() {
        return array.length;
    }

    public long[] getArray() {
        return array;
    }
}
