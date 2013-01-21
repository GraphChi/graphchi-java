package edu.cmu.graphchi.walks.distribution;

import edu.cmu.graphchi.walks.distributions.DiscreteDistribution;
import edu.cmu.graphchi.util.IdCount;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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

        assertEquals(8, merged2.size());

        DiscreteDistribution empty = new DiscreteDistribution();
        DiscreteDistribution mergedWithEmpty = DiscreteDistribution.merge(empty, d1);
        DiscreteDistribution mergedWithEmpty2 = DiscreteDistribution.merge(d2, empty);
        assertTrue(d1 == mergedWithEmpty);
        assertTrue(d2 == mergedWithEmpty2);

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
        Random r = new Random(260379);

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
        DiscreteDistribution mergedDist1 = DiscreteDistribution.merge(leftDist, rightDist);
        DiscreteDistribution mergedDist2 = DiscreteDistribution.merge(rightDist, leftDist);

        for(int i=0; i < 5000; i++) {
            int lc = (leftSet.containsKey(i) ? leftSet.get(i) : 0);
            int rc = (rightSet.containsKey(i) ? rightSet.get(i) : 0);

            assertEquals(lc, leftDist.getCount(i));
            assertEquals(rc, rightDist.getCount(i));
            assertEquals(lc + rc, mergedDist1.getCount(i));
            assertEquals(lc + rc, mergedDist2.getCount(i));

        }
    }

    private void insertMultiple(ArrayList<Integer> arr, int val, int n) {
        for(int i=0; i<n; i++) arr.add(val);
    }

    @Test
    public void testTop() {
        Random r = new Random(260379);

        ArrayList<Integer> workArr = new ArrayList<Integer>();
        TreeMap<Integer, Integer> countToId = new TreeMap<Integer, Integer>(new Comparator<Integer>() {
            public int compare(Integer integer, Integer integer1) {
                return -integer.compareTo(integer1);
            }
        });
        for(int i=1; i < 200; i+=2) {
            int n;
            do {
                n = r.nextInt(10000);
            } while (countToId.containsKey(n)); // Unique count keys
            countToId.put(n, i);
            insertMultiple(workArr, i, n);
        }

        DiscreteDistribution dist = new DiscreteDistribution(toIntArray(workArr));

        IdCount[] top = dist.getTop(10);


        int j = 0;
        for(Map.Entry <Integer, Integer> e : countToId.entrySet()) {
            IdCount topEntryJ = top[j];
            assertEquals((int)e.getValue(), topEntryJ.id);
            assertEquals((int)e.getKey(), topEntryJ.count);
            j++;
            if (top.length <= j) {
                assertEquals(10, j);
                break;
            }
        }
    }

    @Test
    public void testTopWithOnes() {
        DiscreteDistribution d1 = new DiscreteDistribution(new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,20,21,22,23,200,298});
        DiscreteDistribution avoidDist = DiscreteDistribution.createAvoidanceDistribution(new int[]{12, 15, 20});
        IdCount[] top = d1.getTop(10);
        assertEquals(10, top.length);

        DiscreteDistribution mergedWithAvoid = DiscreteDistribution.merge(d1, avoidDist);
        IdCount[] top2 = mergedWithAvoid.getTop(10);
        assertEquals(10, top2.length);
    }

    @Test
    public void testFiltering() {
        DiscreteDistribution d1 = new DiscreteDistribution(new int[] {1,1,1,8,8,8,9,22,22,22,22,22,333,333,333,333,333,333,333,333});
        assertEquals(3, d1.getCount(1));
        assertEquals(3, d1.getCount(8));
        assertEquals(1, d1.getCount(9));
        assertEquals(5, d1.getCount(22));
        assertEquals(8, d1.getCount(333));

        DiscreteDistribution notReallyFiltered = d1.filteredAndShift(1);
        assertTrue(notReallyFiltered == d1);

        DiscreteDistribution filtered = d1.filteredAndShift(4);
        assertEquals(0, filtered.getCount(1));
        assertEquals(0, filtered.getCount(8));
        assertEquals(0, filtered.getCount(9));
        assertEquals(5 - 3, filtered.getCount(22));
        assertEquals(8 - 3, filtered.getCount(333));
        assertEquals(2, filtered.size());

        // Check that avoided ones are not filtered
        DiscreteDistribution filteredWithAnAvoid = DiscreteDistribution.merge(d1,
                DiscreteDistribution.createAvoidanceDistribution(new int[]{99, 108})).filteredAndShift(4);

        assertEquals(0, filteredWithAnAvoid.getCount(1));
        assertEquals(0, filteredWithAnAvoid.getCount(8));
        assertEquals(0, filteredWithAnAvoid.getCount(9));
        assertEquals(5 - 3, filteredWithAnAvoid.getCount(22));
        assertEquals(8 - 3, filteredWithAnAvoid.getCount(333));
        assertEquals(-1, filteredWithAnAvoid.getCount(99));
        assertEquals(-1, filteredWithAnAvoid.getCount(108));

        DiscreteDistribution filteredAll = d1.filteredAndShift(100);
        DiscreteDistribution filteredAll2 = filtered.filteredAndShift(100);

        assertEquals(0, filteredAll.getCount(1));
        assertEquals(0, filteredAll.getCount(8));
        assertEquals(0, filteredAll.getCount(9));
        assertEquals(0, filteredAll.getCount(22));
        assertEquals(0, filteredAll.getCount(333));
        assertEquals(0, filteredAll2.getCount(1));
        assertEquals(0, filteredAll2.getCount(8));
        assertEquals(0, filteredAll2.getCount(9));
        assertEquals(0, filteredAll2.getCount(22));
        assertEquals(0, filteredAll2.getCount(333));

        assertEquals(0, filteredAll.size());
        assertEquals(0, filteredAll2.size());
    }

    @Test
    public void testAvoidance() {
        Random r = new Random(260379);

        /* First create some data  */
        ArrayList<Integer> workArr = new ArrayList<Integer>();
        TreeMap<Integer, Integer> countToId = new TreeMap<Integer, Integer>(new Comparator<Integer>() {
            public int compare(Integer integer, Integer integer1) {
                return -integer.compareTo(integer1);
            }
        });
        for(int i=1; i < 200; i++) {
            int n;
            do {
                n = r.nextInt(10000);
            } while (countToId.containsKey(n)); // Unique count keys
            countToId.put(n, i);
            insertMultiple(workArr, i, n);
        }
        DiscreteDistribution dist = new DiscreteDistribution(toIntArray(workArr));


        /* Then insert some edges to avoid */
        int[] avoids = new int[] {0, 2, 4, 32,33, 66, 67,68,  99, 102, 184};
        DiscreteDistribution avoidDistr = DiscreteDistribution.createAvoidanceDistribution(avoids);

        // Test the merge works both ways
        DiscreteDistribution mergedL = DiscreteDistribution.merge(dist, avoidDistr);
        DiscreteDistribution mergedR = DiscreteDistribution.merge(avoidDistr, dist);

        for(int a : avoids) {
            assertEquals(-1, avoidDistr.getCount(a));
            assertEquals(-1, mergedL.getCount(a));
            assertEquals(-1, mergedR.getCount(a));
        }

        IdCount[] top = dist.getTop(10);
        int j = 0;
        HashSet<Integer> avoidSet = new HashSet<Integer>();
        for(int a : avoids) avoidSet.add(a);
        for(Map.Entry <Integer, Integer> e : countToId.entrySet()) {
            IdCount topEntryJ = top[j];

            if (!avoidSet.contains(e.getKey())) {
                assertEquals((int)e.getValue(), topEntryJ.id);
                assertEquals((int)e.getKey(), topEntryJ.count);
                j++;
                if (top.length <= j) {
                    assertEquals(10, j);
                    break;
                }
            }
        }
    }
}
