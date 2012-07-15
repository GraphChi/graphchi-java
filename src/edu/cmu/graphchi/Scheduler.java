package edu.cmu.graphchi;

/**
 * @author akyrola
 *         Date: 7/15/12
 */
public interface Scheduler {

    public void addTask(int vertexId);

    public void removeTasks();

    public void addAllTasks();

    public boolean hasTasks();
}
