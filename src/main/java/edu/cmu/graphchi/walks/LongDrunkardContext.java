package edu.cmu.graphchi.walks;

/**
 * @author Aapo Kyrola
 */
public interface LongDrunkardContext extends DrunkardContext {

    /**
     * Moves walk to next vertex
     * @param walk walk identified
     * @param destinationVertex vertex id to move hte walk to
     * @param trackBit set to true if this walk should be tracked, otherwise false
     */
    void forwardWalkTo(long walk, int destinationVertex, boolean trackBit);

    void resetWalk(long walk, boolean trackBit);

    /**
     * Reads the track-bit of a walk identifier.
     * @param walk
     * @return
     */
    boolean getTrackBit(long walk);

    /**
     * Returns true if walk was started from the vertex
     */
    boolean isWalkStartedFromVertex(long walk);
}
