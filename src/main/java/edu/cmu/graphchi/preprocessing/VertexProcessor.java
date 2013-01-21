package edu.cmu.graphchi.preprocessing;

/**
 */
public interface VertexProcessor  <ValueType> {

    ValueType receiveVertexValue(int vertexId, String token);

}
