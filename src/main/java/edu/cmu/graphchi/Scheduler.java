package edu.cmu.graphchi;

/**
 * Represents a scheduler object. GraphChi currently supports selective
 * scheduling: all vertices 'scheduled' are updated when the
 * next opportunity arises.
 * Scheduler can be accessed via GraphChiContext.
 * @see edu.cmu.graphchi.engine.BitsetScheduler for an example implementation
 * @author akyrola
 */
public interface Scheduler {

    /**
     * Ask a vertex to be updated.
     * @param vertexId
     */
    public void addTask(int vertexId);

    public void removeTasks(int from, int to);

    public void addAllTasks();

    public boolean hasTasks();

    public boolean isScheduled(int i);

    public void removeAllTasks();

    void scheduleOutNeighbors(ChiVertex vertex);

    void scheduleInNeighbors(ChiVertex vertex);

}
