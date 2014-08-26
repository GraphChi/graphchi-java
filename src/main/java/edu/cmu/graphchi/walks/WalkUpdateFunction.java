package edu.cmu.graphchi.walks;

import edu.cmu.graphchi.ChiVertex;

import java.util.Random;

/**
 * Interface for classes that forward (random) walks.
 * @author Aapo Kyrola
 */
public interface WalkUpdateFunction<VertexDataType, EdgeDataType> {

    /**
     * Called for each source vertex. Return an int-array of vertices to which walk visits should not
     * be tracked. For example, if you are not interested about the walks to the immediate neighbors,
     * you should returns an array of the vertex ids of the neighbors.
     * @param vertex
     * @return
     */
    int[] getNotTrackedVertices(ChiVertex<VertexDataType, EdgeDataType> vertex);

    /**
     * Callback
     * @param walks
     * @param vertex
     * @param drunkardContext
     * @param randomGenerator random-generator
     */
    void processWalksAtVertex(WalkArray walks,
                              ChiVertex<VertexDataType, EdgeDataType> vertex,
                              DrunkardContext drunkardContext,
                              Random randomGenerator);
}
