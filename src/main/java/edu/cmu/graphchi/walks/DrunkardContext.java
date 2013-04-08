package edu.cmu.graphchi.walks;

import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

/**
 * @author Aapo Kyrola
 */
public interface DrunkardContext {

    /**
     * Returns whether vertex id is a source of random walks.
     * @return
     */
    boolean isSource();

    /**
     * Returns the index-number of a source vertex.
     * @return source index or -1 if vertex is not a source
     */
    int sourceIndex();


    int getIteration();

    /**
     * Moves walk to next vertex
     * @param walk walk identified
     * @param destinationVertex vertex id to move hte walk to
     * @param trackBit set to true if this walk should be tracked, otherwise false
     */
    void forwardWalkTo(int walk, int destinationVertex, boolean trackBit);

    void resetWalk(int walk, boolean trackBit);

    /**
     * Reads the track-bit of a walk identifier.
     * @param walk
     * @return
     */
    boolean getTrackBit(int walk);

    /**
     * Returns true if walk was started from the vertex
     */
    boolean isWalkStartedFromVertex(int walk);

    /**
     * Object for translating from internal to original vertex ids
     * @return
     */
    VertexIdTranslate getVertexIdTranslate();
}
