package edu.cmu.graphchi.walks;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.EdgeDirection;

import java.util.Arrays;
import java.util.Random;

/**
 * This class takes a vertex with <b>weighted</b> edges and
 * generates an array of random hops. Note: the hops are edge-indices,
 * not the edges themselves.
 */
public class WeightedHopper {



    public static <VT> int[] generateRandomHopsOut(Random r, ChiVertex<VT, Float> vertex, int n) {
        int l = vertex.numOutEdges();
        float[] cumDist = new float[l];
        float prefix = 0.0f;
        for(int i=0; i < l; i++) {
            float x =  vertex.getOutEdgeValue(i);
            cumDist[i] = prefix + x;
            prefix += x;
        }

        int[] hops = new int[n];
        for(int i=0; i < n; i++) {
            float x = prefix * r.nextFloat();
            if (l > 32)  {
                int h = Arrays.binarySearch(cumDist, x);
                if (h < 0) h = -(h + 1);
                hops[i] = h;
            } else {
                // linear scan
                for(int j=0; j < l; j++) {
                    if (cumDist[j] > x) {
                        hops[i] = j;
                        break;
                    }
                }
            }
        }
        return hops;
    }

    public static <VT> int[] generateRandomHopsAliasMethodOut(Random r, ChiVertex<VT, Float> vertex, int n) {
        return generateRandomHopsAliasMethod(r, vertex, n, EdgeDirection.OUT_EDGES, null);

    }

    public static <VT> int[] generateRandomHopsAliasMethod(Random r, ChiVertex<VT, Float> vertex, int n, EdgeDirection edgeDir,
                                                            EdgeWeightMap weightMap) {
        int l = 0;
        switch (edgeDir) {
            case IN_AND_OUT_EDGES: l = vertex.numEdges(); break;
            case OUT_EDGES: l = vertex.numOutEdges(); break;
            case IN_EDGES: l = vertex.numInEdges(); break;
        }

        float[] values = new float[l];
        int[] aliases = new int[l];

        // Compute average
        float sum = 0;
        for(int i=0; i < l; i++) {
            float x = 0.0f;
            switch (edgeDir) {
                case IN_AND_OUT_EDGES: x = vertex.edge(i).getValue(); break;
                case OUT_EDGES: x= vertex.getOutEdgeValue(i); break;
                case IN_EDGES: x = vertex.inEdge(i).getValue(); break;
            }
            if (weightMap != null) {
                x = weightMap.map(x);
            }

            sum += x;
            values[i] = x;
        }

        int[] aboveAverages = new int[l];
        int[] belowAverages = new int[l];
        int aboveIdx = 0;
        int belowIdx = 0;

        // Init stacks
        for(int i=0; i < l; i++) {
            values[i] = values[i] / sum * l;
            if (values[i] < 1.0f) {
                belowAverages[belowIdx++] = i;
            } else {
                aboveAverages[aboveIdx++] = i;
            }
            aliases[i] = -1;
        }

        // Start shoveling
        while(aboveIdx > 0 && belowIdx > 0) {
            int small = belowAverages[--belowIdx];
            int large = aboveAverages[--aboveIdx];
            aliases[small] = large;
            values[large] = (values[large] - (1.0f - values[small]));
            if (values[large] < 1) {
                belowAverages[belowIdx++] = large;
            } else {
                aboveAverages[aboveIdx++] = large;
            }
        }

        while(aboveIdx > 0) {
            values[aboveAverages[--aboveIdx]] = 1.0f;
        }

        while(belowIdx > 0) { // might happen for numerical instability
            values[belowAverages[--belowIdx]] = 1.0f;
        }

        int[] hops = new int[n];
        // Hops
        for(int i=0; i < n; i++) {
            int bucket = r.nextInt(l);
            float val = r.nextFloat();
            if (val < values[bucket]) {
                hops[i] = bucket;
            } else {
                hops[i] = aliases[bucket];
                if (hops[i] < 0) hops[i] = bucket;
            }
        }

        return hops;
    }



    public static interface EdgeWeightMap {
        public float map(float x);
    }
}
