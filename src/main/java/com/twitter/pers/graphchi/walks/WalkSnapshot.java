package com.twitter.pers.graphchi.walks;

/**
 * @author Aapo Kyrola
 */
public interface WalkSnapshot {

    /** Returns walk at vertex, or null if none **/
    int[] getWalksAtVertex(int vertexId);

    int getFirstVertex();

    int getLastVertex();

    public int numWalks();
}
