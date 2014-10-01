package edu.cmu.graphchi.walks;

/**
 * @author Aapo Kyrola
 */
public interface IntDrunkardContext extends DrunkardContext {

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
}
