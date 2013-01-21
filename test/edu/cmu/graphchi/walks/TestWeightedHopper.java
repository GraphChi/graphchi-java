package edu.cmu.graphchi.walks;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.engine.auxdata.VertexDegree;
import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.*;

/**
 */
public class TestWeightedHopper {

    private float[] initWeights(int n) {
        float[] weights = new float[n];
        Random rinit = new Random(4091982);
        for(int i=0; i <weights.length; i++) {
            weights[i] = rinit.nextFloat() * 10.0f;
        }
        return weights;
    }



    @Test
    public void testBasicMethod() {

        final float[] weights = initWeights(1000);

        ChiVertex<Integer, Float> vert = new ChiVertex<Integer, Float>(0, new VertexDegree(0, weights.length)) {
            public Float getOutEdgeValue(int i) {
                return weights[i];
            }

            @Override
            public int getOutEdgeId(int i) {
                return i;
            }

            @Override
            public int numOutEdges() {
                return weights.length;
            }
        };

        int n = 10000000;
        int[] hops = WeightedHopper.generateRandomHopsOut(new Random(260379), vert, n);

        assertEquals(hops.length, n);

        /* Now check the distribution makes sense */
        int[] counts = new int[weights.length];

        for(int i=0; i < hops.length; i++) {
            assertTrue(hops[i] >= 0 && hops[i] < weights.length);
            counts[hops[i]]++;
        }

        float totalWeight = 0.0f;
        for(int j=0; j < weights.length; j++) totalWeight += weights[j];

        for(int j=0; j < weights.length; j++) {
            int expected = (int) (n * weights[j]  / totalWeight);
            assertTrue(Math.abs(expected - counts[j]) < n / weights.length);  // dubious
        }
    }



    @Test
    public void testBasicMethodSmall() {
        final float[] weights = initWeights(18);

        ChiVertex<Integer, Float> vert = new ChiVertex<Integer, Float>(0, new VertexDegree(0, weights.length)) {
            public Float getOutEdgeValue(int i) {
                return weights[i];
            }

            @Override
            public int getOutEdgeId(int i) {
                return i;
            }

            @Override
            public int numOutEdges() {
                return weights.length;
            }
        };

        int n = 10000000;
        int[] hops = WeightedHopper.generateRandomHopsOut(new Random(260379), vert, n);

        assertEquals(hops.length, n);

        /* Now check the distribution makes sense */
        int[] counts = new int[weights.length];

        for(int i=0; i < hops.length; i++) {
            assertTrue(hops[i] >= 0 && hops[i] < weights.length);
            counts[hops[i]]++;
        }

        float totalWeight = 0.0f;
        for(int j=0; j < weights.length; j++) totalWeight += weights[j];

        for(int j=0; j < weights.length; j++) {
            int expected = (int) (n * weights[j]  / totalWeight);
            assertTrue(Math.abs(expected - counts[j]) < n / weights.length);  // dubious
        }
    }


    @Test
    public void testAliasMethod() {

        final float[] weights = initWeights(1000);
        ChiVertex<Integer, Float> vert = new ChiVertex<Integer, Float>(0, new VertexDegree(0, weights.length)) {
            public Float getOutEdgeValue(int i) {
                return weights[i];
            }

            @Override
            public int getOutEdgeId(int i) {
                return i;
            }

            @Override
            public int numOutEdges() {
                return weights.length;
            }
        };

        int n = 10000000;
        int[] hops = WeightedHopper.generateRandomHopsAliasMethodOut(new Random(260379), vert, n);

        assertEquals(hops.length, n);

        /* Now check the distribution makes sense */
        int[] counts = new int[weights.length];

        for(int i=0; i < hops.length; i++) {
            assertTrue(hops[i] >= 0 && hops[i] < weights.length);
            counts[hops[i]]++;
        }

        float totalWeight = 0.0f;
        for(int j=0; j < weights.length; j++) totalWeight += weights[j];


        for(int j=0; j < weights.length; j++) {
            int expected = (int) (n * weights[j]  / totalWeight);
            assertTrue(Math.abs(expected - counts[j]) < n / weights.length);  // dubious
        }
    }


    @Test
    public void testBoth() {
        long t1 = System.currentTimeMillis();
        testBasicMethod();

        long t2 = System.currentTimeMillis();
        testAliasMethod();
        long tt = System.currentTimeMillis();

        System.out.println("Timings: basic=" + (t2 - t1) + " ms, alias=" + (tt - t2));
    }

}
