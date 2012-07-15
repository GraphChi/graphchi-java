package edu.cmu.graphchi.util;

/**
 * @author akyrola
 *         Date: 7/15/12
 */
public class IdCount implements Comparable<IdCount> {
    public int id;
    public int count;

    public IdCount(int id, int count) {
        this.id = id;
        this.count = count;
    }

    public int compareTo(IdCount idCount) {
        return (idCount.count > this.count ? 1 : (idCount.count != this.count ? -1 : 0));
    }
}
