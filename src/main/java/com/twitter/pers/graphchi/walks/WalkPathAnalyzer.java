package com.twitter.pers.graphchi.walks;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Class for computing paths from the walk-files produced
 * by DrunkardMobForPaths
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public class WalkPathAnalyzer {

    private File directory;

    public WalkPathAnalyzer(File directory) {
        this.directory = directory;
        if (!this.directory.isDirectory()) throw new IllegalArgumentException("You must provide a directory");
    }

    /**
     * Currently very dummy implementation. TODO: Make memory efficient and smarter in general.
     * Just for demonstration purposes.
     */
    public void analyze(int numberOfWalks) throws IOException {
        Walk[] paths = new Walk[numberOfWalks];
        for(int i=0; i < paths.length; i++) {
            paths[i] = new Walk();
        }

        String[] walkFiles = directory.list(new FilenameFilter() {
            @Override
            public boolean accept(File file, String s) {
                return s.startsWith("walks_");
            }
        });

        for(String walkFile : walkFiles) {
            System.out.println("Analyze: " + walkFile);
            long walksInFile = new File(directory, walkFile).length() / 10;
            DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(
                    new File(directory, walkFile)), 1024 * 1024 * 50));
            try {
                long i = 0;
                while(i < walksInFile) {
                    if (i % 1000000 == 0) System.out.println(i + " / " + walksInFile);
                    i++;

                    int walkId = dis.readInt();

                    short hop = dis.readShort();
                    int atVertex = dis.readInt();
                    if (walkId < numberOfWalks) {
                        paths[walkId].addWalk(hop, atVertex);
                        System.out.println(walkId + ", " + hop + ", " + atVertex);
                    }
                }
            } catch (EOFException ioe) {
                continue;
            }
            dis.close();
        }

        for(Walk w : paths) {
            System.out.println(w.getPathDescription());
        }
    }

    private static class Walk {

        private ArrayList<Long> path = new ArrayList<Long>(5);

        void addWalk(short hop, int atVertex) {
            long w = atVertex | ((long)hop << 32);
            path.add(w);
        }

        String getPathDescription() {
            /* Super-slow */
            Collections.sort(path);  // Hop is the highest order bit so sorts by hop
            StringBuffer sb = new StringBuffer();
            for(long w : path) {
                sb.append((w & 0xffffffffl) + "-");
            }
            return sb.toString();
        }
    }

    public static void main(String[] args) throws Exception {
        WalkPathAnalyzer analyzer = new WalkPathAnalyzer(new File("."));
        analyzer.analyze(Integer.parseInt(args[0]));
    }
}
