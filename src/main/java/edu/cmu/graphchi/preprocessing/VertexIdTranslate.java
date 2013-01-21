package edu.cmu.graphchi.preprocessing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Translates vertices from original id to internal-id and
 * vice versa.  GraphChi translates original ids to "modulo-shifted"
 * ids and thus effectively shuffles the vertex ids. This will lead
 * likely to a balanced edge distribution over the space of vertex-ids,
 * and thus roughly equal amount of edges in each shard. With this
 * trick, we do not need to first count the edge distribution and divide
 * the shard intervals based on that but can skip that step. As a downside,
 * the vertex ids need to be translated back and forth.
 * @author Aapo Kyrola, akyrola@cs.cmu.edu
 */
public class VertexIdTranslate {

    private int vertexIntervalLength;
    private int numShards;

    protected  VertexIdTranslate() {

    }

    public VertexIdTranslate(int vertexIntervalLength, int numShards) {
        this.vertexIntervalLength = vertexIntervalLength;
        this.numShards = numShards;
    }

    /**
     * Translates original vertex id to internal vertex id
     * @param origId
     * @return
     */
    public int forward(int origId) {
        return (origId % numShards) * vertexIntervalLength + origId / numShards;
    }

    /**
     * Translates internal id to original id
     * @param transId
     * @return
     */
    public int backward(int transId) {
        final int shard = transId / vertexIntervalLength;
        final int off = transId % vertexIntervalLength;
        return off * numShards + shard;
    }

    public int getVertexIntervalLength() {
        return vertexIntervalLength;
    }

    public int getNumShards() {
        return numShards;
    }

    public String stringRepresentation() {
        return "vertex_interval_length=" + vertexIntervalLength + "\nnumShards=" + numShards + "\n";
    }

    public static VertexIdTranslate fromString(String s) {
       if ("none".equals(s)) {
           return identity();
       }
       String[] lines = s.split("\n");
       int vertexIntervalLength = -1;
       int numShards = -1;
       for(String ln : lines) {
            if (ln.startsWith("vertex_interval_length=")) {
                vertexIntervalLength = Integer.parseInt(ln.split("=")[1]);
            } else if (ln.startsWith("numShards=")) {
                numShards = Integer.parseInt(ln.split("=")[1]);
            }
       }

        if (vertexIntervalLength < 0 || numShards < 0) throw new RuntimeException("Illegal format: " + s);

        return new VertexIdTranslate(vertexIntervalLength, numShards);
    }

    public static VertexIdTranslate fromFile(File f) throws IOException {
        int len = (int) f.length();
        byte[] b = new byte[len];
        FileInputStream fis = new FileInputStream(f);
        fis.read(b);
        fis.close();

        return VertexIdTranslate.fromString(new String(b));
    }

    public static VertexIdTranslate identity() {
        return new VertexIdTranslate() {
            @Override
            public int forward(int origId) {
                return origId;
            }

            @Override
            public int backward(int transId) {
                return transId;
            }

            @Override
            public int getVertexIntervalLength() {
                return -1;
            }

            @Override
            public int getNumShards() {
                return -1;
            }

            @Override
            public String stringRepresentation() {
                return "none";
            }
        };
    }

}
