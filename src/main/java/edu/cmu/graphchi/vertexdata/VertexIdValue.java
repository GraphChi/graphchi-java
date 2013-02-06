package edu.cmu.graphchi.vertexdata;

/**
 * Represents vertex id and a value
 * @author Aapo Kyrola
 */

public class VertexIdValue <VertexValueType> {

    private int vertexId;
    private VertexValueType value;

    public VertexIdValue(int vertexId, VertexValueType value) {
        this.vertexId = vertexId;
        this.value = value;
    }

    public int getVertexId() {
        return vertexId;
    }

    public VertexValueType getValue() {
        return value;
    }
}
