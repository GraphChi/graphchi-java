package edu.cmu.graphchi.aggregators;

/**
 *
 */
public interface VertexMapperCallback <VertexDataType>  {

    VertexDataType map(int vertexId, VertexDataType value);

}
