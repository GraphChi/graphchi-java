package com.twitter.pers.shortestpath;

import edu.cmu.graphchi.engine.auxdata.DegreeData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

/**
 *  This class keeps track of the "seeds" from which the bread-first-searches
 *  are initiated.
 *
 *  A. Sarma, S. Gollapudi, M. Najork, and R. Panigrahy:
 *    A sketch-based distance oracle for web-scale graphs. In WSDM, 2010.
 *  @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
public class SketchSet {

    public final int maxDistance = 7; // 3 bits
    private int nSets;
    private int bitsToEncode;
    private HashSet<Integer> chosenSeeds = new HashSet<Integer>(1000);

    private long[] distanceMasks;
    private long[] seedMasks;
    private int[] seedShift;
    private int[] distanceShift;

    private ArrayList<int[]> seeds = new ArrayList<int[]>();

    public SketchSet(int nSets) {
        this.nSets = nSets;
        this.distanceMasks = new long[nSets];
        this.seedMasks = new long[nSets];
        this.seedShift = new int[nSets];
        this.distanceShift = new int[nSets];
    }

    /**
     * Find the greatest index i of cumulativeDegree so that val < cumulativeDegree[i]
     * @param cumulativeDegree
     * @param val
     * @return
     */
    private int findIdx(long[] cumulativeDegree, long val) {
        int idx = Arrays.binarySearch(cumulativeDegree, val);
        if (idx < 0) {
            // Value not found, but: index of the search key, if it is contained in the list; otherwise, (-(insertion point) - 1)
            return -(idx + 1);
        }
        return idx;
    }

    public int[] seeds(int seedSet) {
        return seeds.get(seedSet);
    }

    /**
     * Selects the seeds randomly weighted by in-degree
     * @param graphName
     * @param nVertices
     * @throws IOException
     */
    public void selectSeeds(String graphName, int nVertices) throws IOException {
        long[] cumdegree = new long[nVertices];
        long cumulant = 0;

        /* Load degree data */
        if (graphName != null) {
            DegreeData degreeData = new DegreeData(graphName);
            int v=0;
            while ( v < nVertices) {
                int st = v;
                int en = Math.min(v + 10000000, nVertices) - 1;
                degreeData.load(st, en);
                for(int i=st; i<=en; i++) {
                    cumulant += degreeData.getDegree(i).inDegree;
                    cumdegree[i] = cumulant;
                }

                v += 10000000;
            }
        }  else {
            // Do an array of ones (this is for unit-tests mostly)
            for(int i=0; i<cumdegree.length; i++) cumdegree[i] = i;
            cumulant = cumdegree.length;
        }
        bitsToEncode = 0;
        long totalSeeds = 0;
        Random random = new Random();
        // Actually, should choose seeds from each of the
        // strongly connected components?
        for(int setNum=0; setNum < nSets; setNum++) {
            // TODO: choose weighted by degree?
            int nSeeds = 1 << setNum;
            int[] seedArr = new int[nSeeds];
            seeds.add(seedArr);

            for(int i=0; i<nSeeds; i++) {
                boolean success = false;
                while(!success) {
                    int vertex = findIdx(cumdegree, random.nextLong() % cumulant);
                    success = !chosenSeeds.contains(vertex);
                    if (success) {
                        chosenSeeds.add(vertex);
                        seedArr[i] = vertex;
                    }
                }
            }
            totalSeeds += nSeeds;

            distanceShift[setNum] = bitsToEncode;
            distanceMasks[setNum] = 7l << bitsToEncode;
            bitsToEncode += 3; // distance
            seedShift[setNum] = bitsToEncode;
            seedMasks[setNum] = (setNum > 0 ? (1l << setNum) - 1 : 0l) << bitsToEncode;
            bitsToEncode += setNum;
        }
        System.out.println("Bits to encode: " + bitsToEncode);
        if (bitsToEncode >= 64) throw new IllegalArgumentException("Too many sets to encode in 64 bits!");
        System.out.println("Total seeds: " + totalSeeds);
    }

    public long initialValue() {
        long val = 0l;
        for(int i=0; i<nSets; i++) {
            val = encode(val, i, 0, maxDistance);
        }
        return val;
    }

    public long encode(long current, int seedSet, int seedIndex, int distance) {
        if (distance >= maxDistance) distance = maxDistance;
        current &= ~seedMasks[seedSet];
        current &= ~distanceMasks[seedSet];
        current |= ((long)distance << distanceShift[seedSet]);
        current |= ((long)seedIndex << seedShift[seedSet]);
        return current;
    }

    public int seedIndex(long current, int seedSet) {
        return (int) ((current & seedMasks[seedSet]) >> seedShift[seedSet]);
    }

    public int distance(long current, int seedSet) {
        return (int) ((current & distanceMasks[seedSet]) >> distanceShift[seedSet]);
    }


}
