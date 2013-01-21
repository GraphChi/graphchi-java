package com.twitter.pers.graphchi.walks;

/**
 * @author Aapo Kyrola
 */
public interface WalkSnapshotForPaths {

    /** Returns walk at vertex, or null if none **/
    long[] getWalksAtVertex(int vertexId);

    int getFirstVertex();

    int getLastVertex();
}
