package edu.cmu.graphchi.engine;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.Scheduler;

import java.util.BitSet;

/**
 * Scheduler implementation for "Selective Scheduling". Each vertex in the
 * graph has a bit which is 1 if the vertex should be updated, and 0 otherwise.
 * To obtain the current scheduler during computation, use context.getScheduler().
 * @see edu.cmu.graphchi.GraphChiContext
 * @author akyrola
 */
public class BitsetScheduler implements Scheduler {

    private int nvertices;
    private BitSet bitset;
    private boolean hasNewTasks;

    public BitsetScheduler(int nvertices) {
        this.nvertices = nvertices;
        bitset = new BitSet(nvertices);
    }

    /**
     * Adds a vertex to schedule
     * @param vertexId
     */
    public void addTask(int vertexId) {
        bitset.set(vertexId, true);
        hasNewTasks = true;
    }

    /**
     * Removes vertices in an interval from schedule
     * @param from first vertex to remove
     * @param to last vertex (inclusive)
     */
    public void removeTasks(int from, int to) {
        for(int i=from; i<=to; i++) {
            bitset.set(i, false);
        }
    }

    /**
     * Adds all vertices in the graph to the schedule.
     */
    public void addAllTasks() {
        hasNewTasks = true;
        bitset.set(0, bitset.size(), true);
    }

    /**
     * Whether there are new tasks since previous reset().
     * @return
     */
    public boolean hasTasks() {
        return hasNewTasks;
    }

    /**
     * Is vertex(i) scheduled or not.
     * @param i
     * @return
     */
    public boolean isScheduled(int i) {
        return bitset.get(i);
    }

    /**
     * Sets all bits to zero/
     */
    public void removeAllTasks() {
        bitset.clear();
        hasNewTasks = false;
    }

    @Override
    /**
     * Convenience method for scheduling all out-neighbors of
     * a vertex.
     * @param vertex vertex in question
     */
    public void scheduleOutNeighbors(ChiVertex vertex) {
        int nEdges = vertex.numOutEdges();
        for(int i=0; i < nEdges; i++) addTask(vertex.outEdge(i).getVertexId());
    }

    @Override
    /**
     * Convenience method for scheduling all in-neighbors of
     * a vertex.
     * @param vertex vertex in question
     */
    public void scheduleInNeighbors(ChiVertex vertex) {
        int nEdges = vertex.numInEdges();
        for(int i=0; i < nEdges; i++) addTask(vertex.inEdge(i).getVertexId());
    }

    /**
     * Reset the "hasNewTasks" counter, but does not
     * clear the bits.
     */
    public void reset() {
        hasNewTasks = false;
    }

}
