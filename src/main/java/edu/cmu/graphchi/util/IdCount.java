package edu.cmu.graphchi.util;

import java.io.Serializable;

/**
 * Tuple presenting a vertex-id and a count.
 * @author akyrola
 *         Date: 7/15/12
 */
public class IdCount implements Comparable<IdCount>, Serializable {
    public int id;
    public int count;

    public IdCount(int id, int count) {
        this.id = id;
        this.count = count;
    }

    public int compareTo(IdCount idCount) {
        return (idCount.count > this.count ? 1 : (idCount.count != this.count ? -1 : (idCount.id < this.id ? -1 : 1)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdCount idCount = (IdCount) o;

        if (id != idCount.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }

    public String toString() {
        return id + ": " + count;
    }
}
