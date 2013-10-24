package edu.cmu.graphchi.vertexdata;

/**
 * Represents vertex id and a value
 * @author Aapo Kyrola
 */

public class VertexIdValue <VertexValueType> {

    private long vertexId;
    private VertexValueType value;

    public VertexIdValue(long vertexId, VertexValueType value) {
        this.vertexId = vertexId;
        this.value = value;
    }

    public long getVertexId() {
        return vertexId;
    }

    public VertexValueType getValue() {
        return value;
    }
}
