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
    private DrunkardFactory factory;
    protected int numVertices;

    public DrunkardJob(String name, RemoteDrunkardCompanion companion, int numVertices,
            DrunkardFactory factory) {
        this.name = name;
        this.numVertices = numVertices;
        this.companion = companion;
        this.factory = factory;
    }

    /**
     * Start walks from vertex firstSourceId to firstSourceId + numSources
     * @param firstSourceId
     * @param numSources  how many walks to start from each source
     * @param walksPerSource how many walks to start from each source
     */
    public void configureSourceRangeInternalIds(int firstSourceId, int numSources, int walksPerSource) {
        if (walkManager != null) {
            throw new IllegalStateException("You can configure walks only once!");
        }
        walkManager = factory.createWalkManager(numVertices, numSources);

        for(int i=firstSourceId; i < firstSourceId + numSources; i++) {
            walkManager.addWalkBatch(i, walksPerSource);
        }
    }

    /**
     * Configure walks starting from list of vertices
     * @param walkSources
     * @param walksPerSource
     */
    public void configureWalkSources(List<Integer> walkSources, int walksPerSource) {
        if (walkManager != null) {
            throw new IllegalStateException("You can configure walks only once!");
        }
        walkManager = factory.createWalkManager(numVertices, walkSources.size());
        Collections.sort(walkSources);
        for(int src : walkSources) {
            walkManager.addWalkBatch(src, walksPerSource);
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
