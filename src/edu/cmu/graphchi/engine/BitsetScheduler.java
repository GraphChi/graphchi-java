package edu.cmu.graphchi.engine;

import edu.cmu.graphchi.Scheduler;

import java.util.BitSet;

/**
 * @author akyrola
 *         Date: 7/15/12
 */
public class BitsetScheduler implements Scheduler {

    private int nvertices;
    private BitSet bitset;
    private boolean hasNewTasks;

    public BitsetScheduler(int nvertices) {
        this.nvertices = nvertices;
        bitset = new BitSet(nvertices);

    }

    public void reset() {
        hasNewTasks = false;
    }

    public void addTask(int vertexId) {
        bitset.set(vertexId, true);
        hasNewTasks = true;
    }

    public void removeTasks(int from, int to) {
        for(int i=from; i<=to; i++) {
            bitset.set(i, false);
        }
    }

    public void addAllTasks() {
        hasNewTasks = true;
        for(int i=0; i<nvertices; i++) bitset.set(i, true);
    }

    public boolean hasTasks() {
        return hasNewTasks;
    }

    public boolean isScheduled(int i) {
        return bitset.get(i);
    }
}
