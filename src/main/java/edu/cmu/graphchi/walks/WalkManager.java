package edu.cmu.graphchi.walks;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.Scheduler;
import edu.cmu.graphchi.engine.VertexInterval;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Manager for random walks
 * * Done partially during authors internship at Twitter, Fall 2012.
 * @author Aapo Kyrola,  akyrola@cs.cmu.edu
 */
public abstract class WalkManager {

    protected int MAX_SOURCES;
    protected int bucketSize; // Store walks into buckets for faster retrieval

    protected final static int initialSize = Integer.parseInt(System.getProperty("walkmanager.initial_size", "32"));

    protected int sourceSeqIdx  = 0;
    protected int[] sources;

    protected int[] sourceWalkCounts = null;
    private long totalWalks = 0;

    protected Object[] bucketLocks;
    protected int[] walkIndices;
    protected BitSet sourceBitSet;

    protected int numVertices;
    protected final Timer grabTimer = Metrics.defaultRegistry().newTimer(WalkManager.class, "grab-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);
    protected final Timer dumpTimer = Metrics.defaultRegistry().newTimer(WalkManager.class, "dump-walks", TimeUnit.SECONDS, TimeUnit.MINUTES);
    protected final Timer schedulePopulate = Metrics.defaultRegistry().newTimer(WalkManager.class, "populate-scheduler", TimeUnit.SECONDS, TimeUnit.MINUTES);
    protected final Timer restore = Metrics.defaultRegistry().newTimer(WalkManager.class, "restore", TimeUnit.SECONDS, TimeUnit.MINUTES);

    private static final Logger logger = ChiLogger.getLogger("walk-manager");

    protected GrabbedBucketConsumer bucketConsumer;
    private BufferedWriter log;

    public WalkManager(int numVertices, int numSources) {
        setSourceAndBucketBits();
        this.numVertices = numVertices;
        if (numSources > MAX_SOURCES) throw new IllegalArgumentException("Max sources: " + numSources);
        sources = new int[numSources];
        sourceWalkCounts = new int[numSources];
        sourceBitSet = new BitSet(numVertices);
        logger.info("Initial size for walk bucket: " + initialSize);
        try {
            log = new BufferedWriter(new FileWriter(new File("walkmanager.log")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sets MAX_SOURCES and bucketSize, which may be different for different subclasses.
     */
    protected abstract void setSourceAndBucketBits();

    protected void log(String s) {
        try {
            log.write(s + "\n");
            log.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isSource(int vertexId) {
        return sourceBitSet.get(vertexId);
    }

    public int getVertexSourceIdx(int vertexId) {
        int idx = Arrays.binarySearch(sources, vertexId);
        if (idx < 0) throw new IllegalArgumentException("Vertex was not a source!");
        return idx;
    }

    public int[] getSources() {
        return sources;
    }

    public void setBucketConsumer(GrabbedBucketConsumer bucketConsumer) {
        this.bucketConsumer = bucketConsumer;
    }

    /**
     * Add a set of walks from a source. Note, you must add the walks
     * in sorted order!
     * @param vertex  source vertex
     * @param numWalks
     * @return
     */
    public synchronized int addWalkBatch(int vertex, int numWalks) {
        if (sourceSeqIdx >= sources.length)
            throw new IllegalStateException("You can have a maximum of " + sources.length + " random walk sources");

        if (sourceSeqIdx > 0) {
            if (sources[sourceSeqIdx - 1] > vertex) {
                throw new IllegalArgumentException("You need to add sources in order!");
            }
        }

        sources[sourceSeqIdx] = vertex;
        sourceWalkCounts[sourceSeqIdx] = numWalks;
        totalWalks += numWalks;

        sourceSeqIdx++;
        return sourceSeqIdx - 1;
    }

    protected abstract void expandCapacity(int bucket, int additional);

    public abstract void initializeWalks();

    public long getTotalWalks() {
        return totalWalks;
    }

    public long getNumOfActiveWalks() {
        long s = 0;
        for(int i=0; i<walkIndices.length; i++) {
            s += walkIndices[i];
        }
        return s;
    }

    public abstract WalkSnapshot grabSnapshot(final int fromVertex, final int toVertexInclusive);

    /** Dump to file all walks with more than 0 hop */
    public abstract void dumpToFile(WalkSnapshot snapshot, String filename) throws IOException;

    public void populateSchedulerWithSources(Scheduler scheduler) {
        for(int i=0; i <sources.length; i++) {
            scheduler.addTask(sources[i]);
        }
    }

    public static int getWalkLength(WalkArray w) {
        if (w == null) return 0;
        return w.size();
    }

    public abstract void populateSchedulerForInterval(Scheduler scheduler, VertexInterval interval);
}
