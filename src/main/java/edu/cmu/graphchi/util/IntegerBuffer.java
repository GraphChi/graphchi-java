package edu.cmu.graphchi.util;

/**
 * An integer array buffer. Not thread-safe!
 */
public class IntegerBuffer {

    private int[] buffer;
    private int idx;

    public IntegerBuffer(int initialCapacity) {
        idx = 0;
        buffer = new int[initialCapacity];
    }

    public void add(int x) {
        buffer[idx++] = x;
        if (idx == buffer.length) {
           int[] tmp = new int[buffer.length * 2];
           System.arraycopy(buffer, 0, tmp, 0, buffer.length);
           buffer = tmp;
        }
    }

    public int size() {
        return idx;
    }

    public int[] toIntArray() {
        if (idx == buffer.length) return buffer;
        else {
            int[] tmp = new int[idx];
            System.arraycopy(buffer, 0, tmp, 0, idx);
            return tmp;
        }
    }

    public int memorySizeEst() {
        int OVERHEAD = 64;  // estimate of the java overhead
        return buffer.length * 4 + 4 + OVERHEAD;
    }

}
