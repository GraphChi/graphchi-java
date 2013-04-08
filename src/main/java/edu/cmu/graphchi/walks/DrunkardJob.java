package edu.cmu.graphchi.walks;

import edu.cmu.graphchi.walks.distributions.RemoteDrunkardCompanion;

import java.util.*;

/**
 * Encapsulates a random walk computation.
 */
public class DrunkardJob {
    private String name;
    private WalkManager walkManager;
    private RemoteDrunkardCompanion companion;
    private int numVertices;

    public DrunkardJob(String name, RemoteDrunkardCompanion companion, int numVertices) {
        this.name = name;
        this.numVertices = numVertices;
        this.companion = companion;
    }

    protected WalkManager createWalkManager(int numSources) {
        return new WalkManager(numVertices, numSources);
    }

    /**
     * Start walks from vertex firstSourceId to firstSourceId + numSources
     * @param firstSourceId
     * @param numSources  how many walks to start from each source
     * @param walksPerSource how many walks to start from each source
     */
    public void configureSourceRangeInternalIds(int firstSourceId, int numSources, int walksPerSource) {
        if (this.walkManager != null) {
            throw new IllegalStateException("You can configure walks only once!");
        }
        this.walkManager = createWalkManager(numSources);

        for(int i=firstSourceId; i < firstSourceId + numSources; i++) {
            this.walkManager.addWalkBatch(i, walksPerSource);
        }
    }

    /**
     * Configure walks starting from list of vertices
     * @param walkSources
     * @param walksPerSource
     */
    public void configureWalkSources(List<Integer> walkSources, int walksPerSource) {
        if (this.walkManager != null) {
            throw new IllegalStateException("You can configure walks only once!");
        }
        this.walkManager = createWalkManager(walkSources.size());
        Collections.sort(walkSources);
        for(int src : walkSources) {
            this.walkManager.addWalkBatch(src, walksPerSource);
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RemoteDrunkardCompanion getCompanion() {
        return companion;
    }

    public WalkManager getWalkManager() {
        return walkManager;
    }
}
