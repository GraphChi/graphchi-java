package edu.cmu.graphchi.walks;

import java.io.*;

/**
 * Analyzes walks produced by the DrunkardMob and computes
 * a <b>global distribution</b>
 */
public class WalkDistributionAnalyzer {

    public static double[] analyzeGlobal(final String directory, final String prefix, final int numVertices) throws IOException {
        File dir = new File(directory);
        int[] counts = new int[numVertices];

        /* Get files */
        String[] files = dir.list(new FilenameFilter() {
            @Override
            public boolean accept(File file, String s) {
                return s.startsWith(prefix);
            }
        });

        long totalVisits = 0;

        System.out.println("Analyzing " + files.length + " files.");

        for(String file : files) {
             File resultFile = new File(dir, file);
             DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(resultFile)));
             try {
                 int source = dis.readInt();
                 int dest = dis.readInt();
                 totalVisits++;
                 counts[dest]++;
             } catch (EOFException e) {}
        }

        /* Compute final distribution */
        double[] ranks = new double[numVertices];
        for(int i=0; i < ranks.length; i++) ranks[i] = counts[i] * 1.0 / totalVisits;

        return ranks;
    }

}
