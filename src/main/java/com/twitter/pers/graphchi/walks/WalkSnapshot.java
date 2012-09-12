package com.twitter.pers.graphchi.walks;

/**
 * @author Aapo Kyrola
 */
public interface WalkSnapshot {

    int[] getWalksAtVertex(int vertexId);

    int getFirstVertex();

    int getLastVertex();
}
