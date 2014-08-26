package edu.cmu.graphchi.walks;

public class BucketsToSend {
    public final int firstVertex;
    public final WalkArray walks;
    public final int length;

    BucketsToSend(int firstVertex, WalkArray walks, int length) {
        this.firstVertex = firstVertex;
        this.walks = walks;
        this.length = length;
    }
}
