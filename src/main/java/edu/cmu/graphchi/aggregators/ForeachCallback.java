package edu.cmu.graphchi.aggregators;

/**
 * @author akyrola
 *         Date: 7/13/12
 */
public interface ForeachCallback <VertexDataType> {

   public void callback(int vertexId, VertexDataType vertexValue);

}
