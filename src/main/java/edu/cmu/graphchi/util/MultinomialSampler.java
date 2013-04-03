package edu.cmu.graphchi.util;

import edu.cmu.graphchi.ChiVertex;

import java.util.Random;

/**
 * Samples values from multinomial distribution.
 * @author Aapo Kyrola
 */
public class MultinomialSampler {

    public static <VT> int[] generateSamplesAliasMethod(Random r, float[] weights, int n) {
        int l = weights.length;
        float[] values = new float[l];
        int[] aliases = new int[l];

        // Compute average
        float sum = 0;
        for(int i=0; i < l; i++) {
            float x = weights[i];
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

        int[] samples = new int[n];
        // Hops
        for(int i=0; i < n; i++) {
            int bucket = r.nextInt(l);
            float val = r.nextFloat();
            if (val < values[bucket]) {
                samples[i] = bucket;
            } else {
                samples[i] = aliases[bucket];
                if (samples[i] < 0) samples[i] = bucket;
            }
        }

        return samples;
    }


}
