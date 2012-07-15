package edu.cmu.graphchi;

/**
 * @author akyrola
 *         Date: 7/15/12
 */
public interface Scheduler {

    public void addTask(int vertexId);

    public void removeTasks(int from, int to);

    public void addAllTasks();

    public boolean hasTasks();

    public boolean isScheduled(int i);
}
