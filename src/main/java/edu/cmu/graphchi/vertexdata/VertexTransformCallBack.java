package edu.cmu.graphchi.vertexdata;

/**
 * Transforms vertex value to another
 * @see edu.cmu.graphchi.vertexdata.VertexTransformer
 */
public interface VertexTransformCallBack<VertexDataType>  {

    VertexDataType map(int vertexId, VertexDataType value);

}
