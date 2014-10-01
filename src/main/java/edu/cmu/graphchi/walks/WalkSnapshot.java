package edu.cmu.graphchi.walks;

/**
 * @author Aapo Kyrola
 */
public interface WalkSnapshot {

    int getFirstVertex();

    int getLastVertex();

    void clear(int vertexId);

    public long numWalks();

    public void restoreUngrabbed();

    WalkArray getWalksAtVertex(int vertexId, boolean processed);
}
