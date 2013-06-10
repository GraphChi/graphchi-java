package com.twitter.pers.graph_generator;

import java.io.*;

/**
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public class EdgeListOutput implements GraphOutput {

    private String fileNamePrefix;

    static int partSeq = 0;

    public EdgeListOutput(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    @Override
    public void addEdges(int[] from, int[] to)  {
        try {
            BufferedWriter dos = partitionOut.get();
            int n = from.length;
            for(int i=0; i<n; i++) {
                dos.write(from[i] + "\t" + to[i] + "\n");
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public void finishUp() {
        try {
            partitionOut.get().close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /* Each thread will have a local partition */
    private ThreadLocal<BufferedWriter> partitionOut = new ThreadLocal<BufferedWriter>() {
        @Override
        protected BufferedWriter initialValue() {
            try {
                int thisPartId;
                synchronized (this) {
                    thisPartId = partSeq++;
                }

                String fileName = fileNamePrefix + "-part" + thisPartId;
                return new BufferedWriter(new FileWriter(fileName));
            } catch (Exception err) {
                err.printStackTrace();
                throw new RuntimeException(err);
            }
        }
    };

}
