package edu.cmu.graphchi.util;

/**
 * Container class carrying a vertex id and a float value
 */
public class IdFloat {
    int vertexId;
    float value;

    public IdFloat(int vertexId, float value) {
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
