package edu.cmu.graphchi.walks;

public class IntWalkArray extends WalkArray {
    private int[] array;

    public IntWalkArray(int[] array) {
        this.array = array;
    }

    @Override
    public int size() {
        return array.length;
    }

    public int[] getArray() {
        return array;
    }
}
