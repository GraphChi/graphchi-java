package com.twitter.pers.graphchi.walks.distributions;

import java.util.Arrays;

/**
 * Presents a map from integers to frequencies.
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
public class DiscreteDistribution {

    private int[] ids;
    private int[] counts;
    private int uniqueCount;


    private DiscreteDistribution(int initialCapacity) {
        ids = new int[initialCapacity];
        counts = new int[initialCapacity];
        uniqueCount = initialCapacity;
    }

    /**
     * Constructs a distribution from a sorted list of entries
     * @param sortedIdList
     */
    public DiscreteDistribution(int[] sortedIdList) {
        if (sortedIdList.length == 0) {
            ids = new int[0];
            counts = new int[0];
            return;
        }
        if (sortedIdList.length == 1) {
            ids = new int[] {sortedIdList[0]};
            counts = new int[] {1};
            return;
        }

        /* First count unique */
        uniqueCount = 1;
        for(int i=1; i < sortedIdList.length; i++) {
            if (sortedIdList[i] != sortedIdList[i - 1]) {
                uniqueCount++;
            }
        }

        ids = new int[uniqueCount];
        counts = new int[uniqueCount];

        /* Create counts */
        int idx = 0;
        int curCount = 1;

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


    public static DiscreteDistribution merge(DiscreteDistribution d1, DiscreteDistribution d2) {
        /* Merge style algorithm */

        /* 1. first count number of different pairs */
        int combinedCount = 0;
        int leftIdx = 0;
        int rightIdx = 0;
        final int leftCount = d1.size();
        final int rightCount = d2.size();

        while(leftIdx < leftCount && rightIdx < rightCount) {
            if (d1.ids[leftIdx] == d2.ids[rightIdx]) {
                leftIdx++;
                rightIdx++;
                combinedCount++;
            } else if (d1.ids[leftIdx] < d2.ids[rightIdx]) {
                combinedCount++;
                leftIdx++;
            } else {
                combinedCount++;
                rightIdx++;
            }
        }
        combinedCount += (rightCount - rightIdx);
        combinedCount += (leftCount - leftIdx);

        /* Create new merged distribution by doing the actual merge */
        DiscreteDistribution merged = new DiscreteDistribution(combinedCount);
        leftIdx = 0;
        rightIdx = 0;
        int idx = 0;
        while(leftIdx < leftCount && rightIdx < rightCount) {
            if (d1.ids[leftIdx] == d2.ids[rightIdx]) {
                merged.ids[idx] = d1.ids[leftIdx];
                merged.counts[idx] = d1.counts[leftIdx] + d2.counts[rightIdx];
                leftIdx++;
                rightIdx++;
                idx++;

            } else if (d1.ids[leftIdx] < d2.ids[rightIdx]) {
                merged.ids[idx] = d1.ids[leftIdx];
                merged.counts[idx] = d1.counts[leftIdx];
                idx++;
                leftIdx++;
            } else {
                merged.ids[idx] = d2.ids[rightIdx];
                merged.counts[idx] = d2.counts[rightIdx];
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

}
