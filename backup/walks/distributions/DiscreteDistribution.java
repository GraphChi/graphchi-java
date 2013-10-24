package edu.cmu.graphchi.walks.distributions;

import edu.cmu.graphchi.util.IdCount;

import java.util.*;

/**
 * Presents a map from integers to frequencies.
 * Special distributions for avoidance can be used to exclude
 * certain ids from the distributions in merges.
 * @author Aapo Kyrola, akyrola@cs.cmu.edu
 */
public class DiscreteDistribution {

    private int[] ids;
    private short[] counts;
    private int uniqueCount;


    private DiscreteDistribution(int initialCapacity) {
        ids = new int[initialCapacity];
        counts = new short[initialCapacity];
        uniqueCount = initialCapacity;
    }

    /**
     * Create an empty distribution
     */
    public DiscreteDistribution() {
        this(0);
    }


    public DiscreteDistribution forceToSize(int size) {
        if (size > this.ids.length) return this;
        int[] newIds = new int[size];
        short[] newCounts = new short[size];
        for(int i=0; i < size; i++) {
            newIds[i] = ids[i];
            newCounts[i] = counts[i];
        }
        DiscreteDistribution d = new DiscreteDistribution();
        d.ids = newIds;
        d.counts = newCounts;
        d.uniqueCount = size;
        return d;
    }

    public int totalCount() {
        int tot = 0;
        for(int c=0; c<counts.length; c++) {
           if (counts[c] > 0) tot += counts[c];
        }
        return tot;
    }


    public void print() {
        for(int i=0; i<ids.length; i++) {
            System.out.println("D " + ids[i] + ", " + counts[i]);
        }
    }

    public IdCount[] getTop(int topN) {
        final TreeSet<IdCount> topList = new TreeSet<IdCount>(new Comparator<IdCount>() {
            public int compare(IdCount a, IdCount b) {
                return (a.count > b.count ? -1 : (a.count == b.count ? (a.id < b.id ? -1 : (a.id == b.id ? 0 : 1)) : 1)); // Descending order
            }
        });
        IdCount least = null;
        for(int i=0; i<uniqueCount; i++) {
            if (topList.size() < topN) {
                topList.add(new IdCount(ids[i], counts[i]));
                least = topList.last();
            } else {
                if (counts[i] > least.count) {
                    topList.remove(least);
                    topList.add(new IdCount(ids[i], counts[i]));
                    least = topList.last();
                }
            }
            if (topList.size() > topN) throw new RuntimeException("Top list too big: " + topList.size());

        }

        IdCount[] topArr = new IdCount[topList.size()];
        topList.toArray(topArr);
        return topArr;
    }

    /**
     * Constructs a distribution from a sorted list of entries.
     * @param sortedIdList
     */
    public DiscreteDistribution(int[] sortedIdList) {
        if (sortedIdList.length == 0) {
            ids = new int[0];
            counts = new short[0];
            return;
        }
        if (sortedIdList.length == 1) {
            ids = new int[] {sortedIdList[0]};
            counts = new short[] {1};
            return;
        }

        /* First count unique */
        uniqueCount = 1;
        for(int i=1; i < sortedIdList.length; i++) {
            if (sortedIdList[i] != sortedIdList[i - 1]) {
                uniqueCount++;
                if (sortedIdList[i] < sortedIdList[i - 1]) {
                    throw new RuntimeException("Not ordered! " + sortedIdList[i] + " < " + sortedIdList[i - 1]);
                }
            }
        }

        ids = new int[uniqueCount];
        counts = new short[uniqueCount];

        /* Create counts */
        int idx = 0;
        short curCount = 1;

        for(int i=1; i < sortedIdList.length; i++) {
            if (sortedIdList[i] != sortedIdList[i - 1]) {
                ids[idx] = sortedIdList[i - 1];
                counts[idx] = curCount;
                idx++;
                curCount = 1;
            }  else curCount++;
        }

        ids[idx] = sortedIdList[sortedIdList.length - 1];
        counts[idx] = curCount;
    }


    public DiscreteDistribution filteredAndShift(int minimumCount) {
         return filteredAndShift((short)minimumCount);
    }

    /**
     * Creates a new distribution with all entries with count less than
     * minimumCount removed, and rest changed by - minimumCount. Does not remove avoids
     */
    public DiscreteDistribution filteredAndShift(short minimumCount) {
        if (minimumCount <= 1) {
            return this;
        }
        int toRemove = 0;
        for(int i=0; i < uniqueCount; i++) {
            toRemove += (counts[i] < minimumCount &&  counts[i] > 0 ? 1 : 0);
        }

        if (toRemove == 0) {
            return this;   // We can safely return same instance, as this is immutable
        }

        DiscreteDistribution filteredDist = new DiscreteDistribution(uniqueCount - toRemove);
        int idx = 0;
        for(int i=0; i < uniqueCount; i++) {
            if (counts[i] >= minimumCount || counts[i] == (-1)) {
                filteredDist.ids[idx] = ids[i];
                if ( counts[i] != (-1)) {
                    filteredDist.counts[idx] = (short) (counts[i] - minimumCount + 1);
                } else {
                    filteredDist.counts[idx] = -1;
                }
                idx++;
            }
        }
        return filteredDist;
    }

    /**
     * Create a special avoidance distribution, where each count is -1.
     * @param avoids  sorted list of ids to avoid
     * @return
     */
    public static DiscreteDistribution createAvoidanceDistribution(int[] avoids) {
        DiscreteDistribution avoidDistr = new DiscreteDistribution(avoids);
        Arrays.fill(avoidDistr.counts, (short) -1);
        return avoidDistr;
    }



    public int getCount(int id) {
        int idx = Arrays.binarySearch(ids, id);
        if (idx >= 0) {
            return counts[idx];
        } else {
            return 0;
        }
    }

    public int size() {
        return uniqueCount;
    }

    public int avoidCount() {
        int a = 0;
        for(int i=0; i < counts.length; i++) a += counts[i] >= 0 ? 0 : 1;
        return a;
    }

    public int memorySizeEst() {
        int OVERHEAD = 64; // Estimate of the java memory overhead
        return 6 * counts.length + 4 + OVERHEAD;
    }




    public int max() {
        int mx = Integer.MIN_VALUE;
        for(int i=0; i < counts.length; i++) {
            if (counts[i] > mx) mx = counts[i];
        }
        return mx;
    }

    /**
     * Merge too distribution. If either has a negative entry for some id, it will
     * remain negative in the merged distribution (see createAvoidDistribution).
     * @param d1
     * @param d2
     * @return
     */
    public static DiscreteDistribution merge(DiscreteDistribution d1, DiscreteDistribution d2) {

        if (d1.uniqueCount == 0) return d2;
        if (d2.uniqueCount == 0) return d1;

        /* Merge style algorithm */

        /* 1. first count number of different pairs */
        int combinedUniqueIndices = 0;
        int leftIdx = 0;
        int rightIdx = 0;
        final int leftCount = d1.size();
        final int rightCount = d2.size();

        while(leftIdx < leftCount && rightIdx < rightCount) {
            if (d1.ids[leftIdx] == d2.ids[rightIdx]) {
                leftIdx++;
                rightIdx++;
                combinedUniqueIndices++;
            } else if (d1.ids[leftIdx] < d2.ids[rightIdx]) {
                combinedUniqueIndices++;
                leftIdx++;
            } else {
                combinedUniqueIndices++;
                rightIdx++;
            }
        }
        combinedUniqueIndices += (rightCount - rightIdx);
        combinedUniqueIndices += (leftCount - leftIdx);

        /* Create new merged distribution by doing the actual merge */
        DiscreteDistribution merged = new DiscreteDistribution(combinedUniqueIndices);
        leftIdx = 0;
        rightIdx = 0;
        int idx = 0;
        while(leftIdx < leftCount && rightIdx < rightCount) {
            if (d1.ids[leftIdx] == d2.ids[rightIdx]) {
                merged.ids[idx] = d1.ids[leftIdx];
                merged.counts[idx] = (short) (d1.counts[leftIdx] + d2.counts[rightIdx]);

                // Force to retain negativity
                if (d1.counts[leftIdx] < 0 || d2.counts[rightIdx] < 0) {
                    merged.counts[idx] = -1;
                }
                leftIdx++;
                rightIdx++;
                idx++;

            } else if (d1.ids[leftIdx] < d2.ids[rightIdx]) {
                merged.ids[idx] = d1.ids[leftIdx];
                merged.counts[idx] = d1.counts[leftIdx];   // Note, retains negativity
                idx++;
                leftIdx++;
            } else {
                merged.ids[idx] = d2.ids[rightIdx];
                merged.counts[idx] = d2.counts[rightIdx];   // Note, retains negativity
                idx++;
                rightIdx++;
            }
        }

        while(leftIdx < leftCount) {
            merged.ids[idx] = d1.ids[leftIdx];
            merged.counts[idx] = d1.counts[leftIdx];
            leftIdx++;
            idx++;
        }

        while(rightIdx < rightCount) {
            merged.ids[idx] = d2.ids[rightIdx];
            merged.counts[idx] = d2.counts[rightIdx];
            rightIdx++;
            idx++;
        }
        return merged;
    }

    public int sizeExcludingAvoids() {
        int j = 0;
        for(int i=0; i < uniqueCount; i++) {
            if (counts[i] > 0) j++;
        }
        return j;
    }
}
