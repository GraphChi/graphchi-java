package edu.cmu.graphchi.walks;

/**
 * @author Aapo Kyrola
 */
public interface WalkSnapshot {

    /** Returns walk at vertex, or null if none **/
    int[] getWalksAtVertex(int vertexId, boolean processed);

    int getFirstVertex();

    int getLastVertex();

    void clear(int vertexId);

    public long numWalks();

    public void restoreUngrabbed();
}
