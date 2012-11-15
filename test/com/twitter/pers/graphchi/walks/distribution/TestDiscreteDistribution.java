package com.twitter.pers.graphchi.walks.distribution;

import com.twitter.pers.graphchi.walks.distributions.DiscreteDistribution;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
public class TestDiscreteDistribution {

    @Test
    public void testTrivialConstruction() {
        DiscreteDistribution empty = new DiscreteDistribution(new int[0]);
        assertEquals(0, empty.getCount(0));
        assertEquals(0, empty.getCount(1));

        DiscreteDistribution singleton = new DiscreteDistribution(new int[] {876});
        assertEquals(1, singleton.getCount(876));
        assertEquals(0, singleton.getCount(875));
        assertEquals(0, singleton.getCount(877));
    }

    @Test
    public void testConstruction() {
        DiscreteDistribution d1 = new DiscreteDistribution(new int[] {1,1,1,8,8,8,9,22,22,22,22,22});
        assertEquals(3, d1.getCount(1));
        assertEquals(3, d1.getCount(8));
        assertEquals(1, d1.getCount(9));
        assertEquals(5, d1.getCount(22));
        assertEquals(0, d1.getCount(0));
        assertEquals(0, d1.getCount(10));
        assertEquals(0, d1.getCount(10324324));
    }

    @Test
    public void testMerge() {
        DiscreteDistribution d1 = new DiscreteDistribution(new int[] {1,1,1,8,8,8,9,22,22,22,22,22});
        DiscreteDistribution d2 = new DiscreteDistribution(new int[] {1,1,1,8,8,8,9,22,22,22,22,22});
        DiscreteDistribution merged = DiscreteDistribution.merge(d1, d2);

        assertEquals(3 * 2, merged.getCount(1));
        assertEquals(3 * 2, merged.getCount(8));
        assertEquals(2,     merged.getCount(9));
        assertEquals(5 * 2, merged.getCount(22));
        assertEquals(0, merged.getCount(0));
        assertEquals(0, merged.getCount(10));
        assertEquals(0, merged.getCount(10324324));

        DiscreteDistribution d3 = new DiscreteDistribution(new int[] {1,7,8,1000,1000,1000,2000,2000,30000});
        DiscreteDistribution merged2 = DiscreteDistribution.merge(d1, d3);
        assertEquals(4, merged2.getCount(1));
        assertEquals(1, merged2.getCount(7));
        assertEquals(4, merged2.getCount(8));
        assertEquals(1, merged2.getCount(9));
        assertEquals(5, merged2.getCount(22));
        assertEquals(3, merged2.getCount(1000));
        assertEquals(2, merged2.getCount(2000));
        assertEquals(1, merged2.getCount(30000));
    }


    private int[] toIntArray(ArrayList<Integer> arr) {
       int[] a = new int[arr.size()];
       for(int i=0; i<arr.size(); i++) {
           a[i] = arr.get(i);
       }
       return a;
    }

    @Test
    public void testBigMerge() {
        Random r = new Random();
        TreeMap<Integer, Integer> leftSet = new TreeMap<Integer, Integer>();
        TreeMap<Integer, Integer> rightSet = new TreeMap<Integer, Integer>();

        // There must be collisions and also some omissions
        for(int i=0; i < 4001; i++) {
             leftSet.put(r.nextInt(4000),  r.nextInt(1000));
             rightSet.put(r.nextInt(4000),  r.nextInt(1000));
        }

        // Compose
        ArrayList<Integer> leftArray = new ArrayList<Integer>(4000);
        for(Map.Entry<Integer ,Integer> e : leftSet.entrySet()) {
           for(int j=0; j<e.getValue(); j++) leftArray.add(e.getKey());
        }
        ArrayList<Integer> rightArray = new ArrayList<Integer>(4000);
        for(Map.Entry<Integer ,Integer> e : rightSet.entrySet()) {
            for(int j=0; j<e.getValue(); j++) rightArray.add(e.getKey());
        }



        DiscreteDistribution leftDist = new DiscreteDistribution(toIntArray(leftArray));
        DiscreteDistribution rightDist = new DiscreteDistribution(toIntArray(rightArray));
        DiscreteDistribution mergedDist = DiscreteDistribution.merge(leftDist, rightDist);
        for(int i=0; i < 5000; i++) {
            int lc = (leftSet.containsKey(i) ? leftSet.get(i) : 0);
            int rc = (rightSet.containsKey(i) ? rightSet.get(i) : 0);

            assertEquals(lc, leftDist.getCount(i));
            assertEquals(rc, rightDist.getCount(i));
            assertEquals(lc + rc, mergedDist.getCount(i));
        }
    }
}
