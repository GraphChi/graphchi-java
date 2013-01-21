package edu.cmu.graphchi.util;

/**
 * Tuple containg a vertex-id and an integer number.
 */
public class IdInt {
    int vertexId;
    int value;

    public IdInt(int vertexId, int value) {
        this.vertexId = vertexId;
        this.value = value;
    }

    public int getVertexId() {
        return vertexId;
    }

    public float getValue() {
        return value;
    }
}