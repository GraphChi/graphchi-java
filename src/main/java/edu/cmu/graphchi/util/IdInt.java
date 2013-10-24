package edu.cmu.graphchi.util;

/**
 * Tuple containg a vertex-id and an integer number.
 */
public class IdInt {
    long vertexId;
    int value;

    public IdInt(long vertexId, int value) {
        this.vertexId = vertexId;
        this.value = value;
    }

    public long getVertexId() {
        return vertexId;
    }

    public float getValue() {
        return value;
    }
}