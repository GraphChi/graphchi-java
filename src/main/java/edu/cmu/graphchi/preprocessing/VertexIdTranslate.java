package edu.cmu.graphchi.preprocessing;

/**
 * Translates vertices from original id to a module-shifted
 * id and backwards.
 * @author Aapo Kyrola, akyrola@cs.cmu.edu
 */
public class VertexIdTranslate {

    private int vertexIntervalLength;
    private int numShards;

    public VertexIdTranslate(int vertexIntervalLength, int numShards) {
        this.vertexIntervalLength = vertexIntervalLength;
        this.numShards = numShards;
    }

    public int forward(int origId) {
        return (origId % numShards) * vertexIntervalLength + origId / numShards;
    }

    public int backward(int transId) {
        final int shard = transId / vertexIntervalLength;
        final int off = transId % vertexIntervalLength;
        return off * numShards + shard;
    }

}
