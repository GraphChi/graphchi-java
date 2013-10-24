package edu.cmu.graphchi.walks.analysis;

import java.io.*;
import java.util.*;

/**
 * Template code for computing paths from the walk-files produced
 * by DrunkardMobForPaths
 *
 * Usage:
 *    [directory-containing the walks] [min-walk-id to process] [max-walk-id to process] [maxHops]
 *
 * The idea is to process the walks in such chunks that they can be processed in memory.
 * For example, if you did 1 billion walks of 10 hops, perhaps analyzing 200 million walks a time
 * makes sense, and first run with min-walk-id 0 and max-walkid 200,000,000, then with
 * min-walkid 200,000,001 etc...  (On a machine with ~ 100 gig of memory).
 * CAUTION: Java's memory allocation is a bit hard to estimate, so you need to try out
 * good chunk sizes.
 *
 * Process:
 * 1. compute the path for each walk id
 * 2. group by walk source
 * 3. group walks by source by the destination
 * 4. group by path type (function for returning path-type is just a mock)
 * 5. output the distribution: for source-walk,path-type,count
 *
 * @author Aapo Kyrola, akyrola@cs.cmu.edu
 */
public class WalkPathAnalyzerTemplate {

    private File directory;
    private BufferedWriter output;

    public WalkPathAnalyzerTemplate(File directory) throws IOException {
        this.directory = directory;
        if (!this.directory.isDirectory()) throw new IllegalArgumentException("You must provide a directory");

        output = new BufferedWriter(new FileWriter("walkoutput"));

    }

    /**
     * Currently very dummy implementation. TODO: Make memory efficient and smarter in general.
     * Just for demonstration purposes.
     */
    public void analyze(int minWalkId, int maxWalkId, int maxHops) throws IOException {
        int numberOfWalks = maxWalkId - minWalkId + 1;
        Walk[] paths = new Walk[numberOfWalks];
        for(int i=0; i < paths.length; i++) {
            paths[i] = new Walk(maxHops);
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
                    if (walkId >= minWalkId && walkId <= maxWalkId) {
                        paths[walkId - minWalkId].addWalk(hop, atVertex);
                    }
                }
            } catch (EOFException ioe) {
                continue;
            }
            dis.close();
        }

        for(Walk w : paths) {
            w.sort();
        }

        /* Analyze the walks */
        groupBySourceAndAnalyze(paths);

        output.close();
    }

    /**
     * Since all walk-ids from a given source are in a consequent
     * interval, we can easily select walks from one source, and
     * then analyze them based on destination.
     * @param paths
     */
    private void groupBySourceAndAnalyze(Walk[] paths) {
        int curSource = -1;
        ArrayList<Walk> curSet = new ArrayList<Walk>();
        for(int j=0; j < paths.length; j++) {
            Walk w = paths[j];
            if (w.getSource() != curSource) {
                if (curSource != -1) processWalksFromSource(curSource, curSet);
                curSource = w.getSource();
                curSet = new ArrayList<Walk>();
            }
            curSet.add(w);
        }
        // Last
        processWalksFromSource(curSource, curSet);
    }

    private void processWalksFromSource(int source, ArrayList<Walk> walksFromSource) {
        // Now sort by target
        Collections.sort(walksFromSource, new Comparator<Walk>() {
            @Override
            public int compare(Walk walk1, Walk walk2) {
                int dest1 = walk1.getDestination();
                int dest2 = walk2.getDestination();
                return (dest1 == dest2 ?  0  : (dest1 < dest2 ? -1 : 1)) ;
            }
        });

        // Group by target
        int curDest = -1;
        ArrayList<Walk> curSet = new ArrayList<Walk>();
        for(Walk w : walksFromSource) {
            if (w.getDestination() != curDest) {
                if (curDest != -1) handleSourcePathSet(source, curDest, curSet);
                curDest = w.getDestination();
                curSet = new ArrayList<Walk>();
            }
            curSet.add(w);
        }
    }


    /**
     * Now we have a set of walks that share source and destination.
     * Compute distribution of path types.
     * @param pathSet
     */
    private void handleSourcePathSet(int source, int dst, ArrayList<Walk> pathSet) {
        HashMap<String, Integer> pathDist = new HashMap<String, Integer>();
        for(Walk w : pathSet) {
            String pathType = getPathType(w);
            if (pathDist.containsKey(pathType)) {
                pathDist.put(pathType, pathDist.get(pathType) + 1);
            } else {
                pathDist.put(pathType, 1);
            }
        }

        try {
            // Write the distribution out to a file. You probably want to replace this.
            for(Map.Entry<String, Integer> pathCount : pathDist.entrySet()) {
                output.write(source + "-" + dst +"," + pathCount.getKey() + "," + pathCount.getValue() + "\n");
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }


    // Mock implementation of get-path-type
    // TODO
    public String getPathType(Walk w) {
        // Fake!
        int[] p = w.getPath();
        StringBuffer sb = new StringBuffer();
        for(int j=0; j<p.length; j++) {
            sb.append(p[j] % 2);
            sb.append("-");
        }
        return sb.toString();
    }


    private static class Walk {

        private long[] path;
        int idx;

        private Walk(int maxHops) {
            idx = 0;
            path = new long[maxHops];
        }

        void addWalk(short hop, int atVertex) {
            long w = atVertex | ((long)hop << 32);
            if (idx < path.length) path[idx++] = w;
        }

        int getSource() {
            return (int) (path[0] & 0xffffffffl);
        }

        int getDestination() {
            return (int) (path[idx - 1] & 0xffffffffl);
        }

        int[] getPath() {
            int[] p = new int[idx];
            for(int i=0; i<idx; i++) {
                p[i] = (int) (path[i] & 0xffffffffl);
            }
            return p;
        }

        void sort() {
            Arrays.sort(path);
        }

        String getPathDescription() {
            /* Super-slow */
            Arrays.sort(path);  // Hop is the highest order bit so sorts by hop
            StringBuffer sb = new StringBuffer();
            for(long w : path) {
                sb.append((w & 0xffffffffl) + "-");
            }
            return sb.toString();
        }
    }

    // Usage:
    //  [directory-containing the walks] [min-walk-id] [max-walk-id to process] [maxHops]
    public static void main(String[] args) throws Exception {

        WalkPathAnalyzerTemplate analyzer = new WalkPathAnalyzerTemplate(new File(args[0]));
        int minWalkId = Integer.parseInt(args[1]);
        int maxWalkId = Integer.parseInt(args[2]);
        int maxHops = Integer.parseInt(args[3]);

        analyzer.analyze(minWalkId, maxWalkId, maxHops);
    }
}
