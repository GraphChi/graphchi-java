package edu.cmu.graphchi.preprocessing;

/**
 * Converts a vertex-value from string to the valuetype.
 */
public interface VertexProcessor  <ValueType> {

    ValueType receiveVertexValue(int vertexId, String token);

}
