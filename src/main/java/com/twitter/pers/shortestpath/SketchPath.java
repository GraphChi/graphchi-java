package com.twitter.pers.shortestpath;

/**
 * Represent a path between two vertices via
 * a seed.
 */
public class SketchPath {

    private int fromVertex;
    private int destVertex;
    private int viaSeed;
    private int distanceFromVertexToSeed;
    private int distanceFromSeedToDest;

    public SketchPath(int fromVertex, int destVertex, int viaSeed, int distanceFromVertexToSeed, int distanceFromSeedToDest) {
        this.fromVertex = fromVertex;
        this.destVertex = destVertex;
        this.viaSeed = viaSeed;
        this.distanceFromVertexToSeed = distanceFromVertexToSeed;
        this.distanceFromSeedToDest = distanceFromSeedToDest;
    }

    public int getFromVertex() {
        return fromVertex;
    }

    public void setFromVertex(int fromVertex) {
        this.fromVertex = fromVertex;
    }

    public int getDestVertex() {
        return destVertex;
    }

    public void setDestVertex(int destVertex) {
        this.destVertex = destVertex;
    }

    public int getViaSeed() {
        return viaSeed;
    }

    public void setViaSeed(int viaSeed) {
        this.viaSeed = viaSeed;
    }

    public int getDistance() {
        if (distanceFromSeedToDest == Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return distanceFromSeedToDest + distanceFromVertexToSeed;
    }

    public int getDistanceFromVertexToSeed() {
        return distanceFromVertexToSeed;
    }

    public void setDistanceFromVertexToSeed(int distanceFromVertexToSeed) {
        this.distanceFromVertexToSeed = distanceFromVertexToSeed;
    }

    public int getDistanceFromSeedToDest() {
        return distanceFromSeedToDest;
    }

    public void setDistanceFromSeedToDest(int distanceFromSeedToDest) {
        this.distanceFromSeedToDest = distanceFromSeedToDest;
    }
}
