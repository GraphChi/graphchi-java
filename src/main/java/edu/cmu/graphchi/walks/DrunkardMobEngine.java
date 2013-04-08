package edu.cmu.graphchi.walks;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.walks.distributions.RemoteDrunkardCompanion;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Class for running DrunkardMob random walk applications.
 * This can run multiple distinct random walk computations. They are executed
 * simultaneously when iterating over the graph.
 * @author Aapo Kyrola
 */
public class DrunkardMobEngine<VertexDataType, EdgeDataType> {

    protected GraphChiEngine<VertexDataType, EdgeDataType> engine;
    protected List<DrunkardDriver> drivers;

    protected static Logger logger = ChiLogger.getLogger("drunkardmob-engine");


    public DrunkardMobEngine(String baseFilename, int nShards) throws IOException {
        createGraphChiEngine(baseFilename, nShards);
        this.drivers = new ArrayList<DrunkardDriver>();

        // Disable all edge directions by default
        engine.setDisableInedges(true);
        engine.setDisableOutEdges(true);
        engine.setModifiesInedges(false);
        engine.setModifiesOutedges(false);
    }

    protected void createGraphChiEngine(String baseFilename, int nShards) throws IOException {
        this.engine = new GraphChiEngine<VertexDataType, EdgeDataType>(baseFilename, nShards);
        this.engine.setOnlyAdjacency(true);
        this.engine.setVertexDataConverter(null);
        this.engine.setEdataConverter(null);
    }


    public GraphChiEngine<VertexDataType, EdgeDataType> getEngine() {
        return engine;
    }

    public DrunkardJob getJob(String name) {
        for(DrunkardDriver driver : drivers) {
            if (driver.job.getName().equals(name)) {
                return driver.job;
            }
        }
        return null;
    }

    /**
     * Configure edge data type converter - if you need edge values
     * @param edataConverter
     */
    public void setEdataConverter(BytesToValueConverter<EdgeDataType> edataConverter) {
        engine.setEdataConverter(edataConverter);
        if (edataConverter != null) {
            engine.setOnlyAdjacency(false);
        } else {
            engine.setOnlyAdjacency(true);
        }
    }

    /**
     * Configure vertex data type converter - if you need vertex values
     * @param vertexDataConverter
     */
    public void setVertexDataConverter(BytesToValueConverter<VertexDataType> vertexDataConverter) {
        engine.setVertexDataConverter(vertexDataConverter);
    }


    protected WalkManager createWalkManager(int numSources) {
        return new WalkManager(engine.numVertices(), numSources);
    }

    /**
     * Adds a random walk job.  Use run() to run all the jobs.
     * @param edgeDirection which direction edges need to be considered
     * @param callback your walk logic
     * @param companion object that keeps track of the walks
     * @return the job object
     */
    public DrunkardJob addJob(String jobName, EdgeDirection edgeDirection,
                              WalkUpdateFunction<VertexDataType, EdgeDataType> callback,
                              RemoteDrunkardCompanion companion) throws IOException {

        /* Configure engine parameters */
        switch(edgeDirection) {
            case IN_AND_OUT_EDGES:
                engine.setDisableInedges(false);
                engine.setDisableOutEdges(false);
                break;
            case IN_EDGES:
                engine.setDisableInedges(false);
                break;
            case OUT_EDGES:
                engine.setDisableOutEdges(false);
                break;
        }

        /**
         * Create job object and the driver-object.
         */
        DrunkardJob job = new DrunkardJob(jobName, companion);
        drivers.add(new DrunkardDriver(job, callback));
        return job;
    }

    public void run(int numIterations) throws IOException, RemoteException {
        engine.setEnableScheduler(true);

        int memoryBudget = 1200;
        if (System.getProperty("membudget") != null) memoryBudget = Integer.parseInt(System.getProperty("membudget"));

        engine.setMemoryBudgetMb(memoryBudget);
        engine.setEnableDeterministicExecution(false);
        engine.setAutoLoadNext(false);
        engine.setVertexDataConverter(null);
        engine.setMaxWindow(10000000); // Handle maximum 10M vertices a time.

        for(DrunkardDriver driver : drivers) {
            if (driver.job.walkManager == null) {
                throw new IllegalStateException("You need to configure walks by calling DrunkardJob.configureXXX()");
            }
            driver.initWalks();
        }

        /* Run GraphChi */
        logger.info("Starting running drunkard jobs (" + drivers.size() + " jobs)");
        engine.run(new GraphChiDrunkardWrapper(), numIterations);

        /* Finish up */
        for(DrunkardDriver driver: drivers) {
            driver.spinUntilFinish();
        }
    }

    public VertexIdTranslate getVertexIdTranslate() {
        return engine.getVertexIdTranslate();
    }


    /**
     * Multiplex for DrunkardDrivers.
     */
    protected class GraphChiDrunkardWrapper implements GraphChiProgram<VertexDataType, EdgeDataType> {
        @Override
        public void update(ChiVertex<VertexDataType, EdgeDataType> vertex, GraphChiContext context) {

            /* Buffer management. TODO: think, this is too complex after adding the multiplex */
            if (context.getThreadLocal() == null) {
                ArrayList<LocalWalkBuffer> multiplexedLocalBuffers = new ArrayList<LocalWalkBuffer>(drivers.size());
                for(DrunkardDriver driver: drivers) {
                    LocalWalkBuffer buf = new  LocalWalkBuffer();
                    driver.addLocalBuffer(buf);
                    multiplexedLocalBuffers.add(buf);
                }
                context.setThreadLocal(multiplexedLocalBuffers);
            }

            final ArrayList<LocalWalkBuffer> multiplexedLocalBuffers = (ArrayList<LocalWalkBuffer>) context.getThreadLocal();

            int i = 0;
            for(DrunkardDriver driver : drivers) {
                driver.update(vertex, context, multiplexedLocalBuffers.get(i++));
            }
        }

        @Override
        public void beginIteration(GraphChiContext ctx) {
            for(DrunkardDriver driver : drivers) {
                driver.beginIteration(ctx);
            }
        }

        @Override
        public void endIteration(GraphChiContext ctx) {
            for(DrunkardDriver driver : drivers) {
                driver.endIteration(ctx);
            }
        }

        @Override
        public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
            for(DrunkardDriver driver : drivers) {
                driver.beginInterval(ctx, interval);
            }
        }

        @Override
        public void endInterval(GraphChiContext ctx, VertexInterval interval) {
            for(DrunkardDriver driver : drivers) {
                driver.endInterval(ctx, interval);
            }
        }

        @Override
        public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
            for(DrunkardDriver driver : drivers) {
                driver.beginSubInterval(ctx, interval);
            }
        }

        @Override
        public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
            for(DrunkardDriver driver : drivers) {
                driver.endSubInterval(ctx, interval);
            }
        }
    }

    /**
     * Inner class to encapsulate the graphchi program running the show.
     * Due to several optimizations, it is quite complicated!
     */
    protected class DrunkardDriver implements GrabbedBucketConsumer {
        private WalkSnapshot curWalkSnapshot;
        private final DrunkardJob job;

        private LinkedBlockingQueue<BucketsToSend> bucketQueue = new LinkedBlockingQueue<BucketsToSend>();
        private boolean finished = false;
        private Thread dumperThread;
        private AtomicLong pendingWalksToSubmit = new AtomicLong(0);
        WalkUpdateFunction<VertexDataType, EdgeDataType> callback;

        private final Timer purgeTimer =
                Metrics.defaultRegistry().newTimer(DrunkardMobEngine.class, "purge-localwalks", TimeUnit.SECONDS, TimeUnit.MINUTES);


        DrunkardDriver(final DrunkardJob job, WalkUpdateFunction<VertexDataType, EdgeDataType> callback) {
            this.job = job;
            this.callback = callback;

            // Setup thread for sending walks to the companion (i.e tracker)
            // Launch a thread to send to the companion
            dumperThread = new Thread(new Runnable() {
                public void run() {
                    int[] walks = new int[256 * 1024];
                    int[] vertices = new int[256 * 1024];
                    int idx = 0;

                    while(!finished || bucketQueue.size() > 0) {
                        BucketsToSend bucket = null;
                        try {
                            bucket = bucketQueue.poll(1000, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                        }
                        if (bucket != null) {
                            pendingWalksToSubmit.addAndGet(-bucket.length);
                            for(int i=0; i<bucket.length; i++) {
                                int w = bucket.walks[i];
                                int v = WalkManager.off(w) + bucket.firstVertex;


                                // Skip walks with the track-bit (hop-bit) not set
                                boolean trackBit = WalkManager.hop(w);

                                if (!trackBit) {
                                    continue;
                                }

                                walks[idx] = w;
                                vertices[idx] = v;
                                idx++;

                                if (idx >= walks.length) {
                                    try {
                                        job.companion.processWalks(walks, vertices);
                                    } catch (Exception err) {
                                        err.printStackTrace();
                                    }
                                    idx = 0;
                                }

                            }
                        }
                    }

                    // Send rest
                    try {
                        int[] tmpWalks = new int[idx];
                        int[] tmpVertices = new int[idx];
                        System.arraycopy(walks, 0, tmpWalks, 0, idx);
                        System.arraycopy(vertices, 0, tmpVertices, 0, idx);
                        job.companion.processWalks(tmpWalks, tmpVertices);
                    } catch (Exception err) {
                        err.printStackTrace();
                    }
                }
            });
            dumperThread.start();
        }



        public void update(ChiVertex<VertexDataType, EdgeDataType> vertex, final GraphChiContext context,
                           final LocalWalkBuffer localBuf) {


            try {
                // Flow control
                while (pendingWalksToSubmit.get() > job.walkManager.getTotalWalks() / 40) {
                    //System.out.println("Too many walks waiting for delivery: " + pendingWalksToSubmit.get());
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }
                }

                boolean  firstIteration = (context.getIteration() == 0);
                int[] walksAtMe = curWalkSnapshot.getWalksAtVertex(vertex.getId(), true);

                // Very dirty memory management
                curWalkSnapshot.clear(vertex.getId());

                // On first iteration, we ask the callback for list of vertices
                // that should not be tracked
                if (firstIteration) {
                    if (job.walkManager.isSource(vertex.getId())) {
                        int mySourceIdx = job.walkManager.getVertexSourceIdx(vertex.getId());

                        job.companion.setAvoidList(mySourceIdx, callback.getNotTrackedVertices(vertex));
                    }
                }
                if (walksAtMe == null || walksAtMe.length == 0) return;

                Random randomGenerator = localBuf.random;

                final boolean  isSource = job.walkManager.isSource(vertex.getId());
                final int mySourceIndex = (isSource ? job.walkManager.getVertexSourceIdx(vertex.getId()) : -1);

                callback.processWalksAtVertex(walksAtMe, vertex, new DrunkardContext() {
                    @Override
                    public boolean isSource() {
                        return isSource;
                    }

                    @Override
                    public int sourceIndex() {
                        return mySourceIndex;
                    }

                    @Override
                    public int getIteration() {
                        return context.getIteration();
                    }

                    @Override
                    public void forwardWalkTo(int walk, int destinationVertex, boolean trackBit) {
                        localBuf.add(WalkManager.sourceIdx(walk), destinationVertex, trackBit);
                    }

                    @Override
                    public void resetWalk(int walk, boolean trackBit) {
                        forwardWalkTo(walk, job.walkManager.getSourceVertex(WalkManager.sourceIdx(walk)), false);
                    }

                    @Override
                    public boolean getTrackBit(int walk) {
                        return WalkManager.hop(walk);
                    }

                    @Override
                    public boolean isWalkStartedFromVertex(int walk) {
                        return mySourceIndex == WalkManager.sourceIdx(walk);
                    }

                    @Override
                    public VertexIdTranslate getVertexIdTranslate() {
                        return getVertexIdTranslate();
                    }
                }, randomGenerator);
            } catch (RemoteException re) {
                throw new RuntimeException(re);
            }
        }

        public void initWalks() throws RemoteException{
            job.walkManager.initializeWalks();
            job.getCompanion().setSources(job.walkManager.getSources());
        }




        public void beginIteration(GraphChiContext ctx) {
            if (ctx.getIteration() == 0) {
                ctx.getScheduler().removeAllTasks();
                job.walkManager.populateSchedulerWithSources(ctx.getScheduler());
            }
        }

        public void endIteration(GraphChiContext ctx) {}

        public void spinUntilFinish() {
            finished = true;
            while (bucketQueue.size() > 0) {
                try {
                    System.out.println("Waiting ..." + bucketQueue.size());
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                dumperThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private ArrayList<LocalWalkBuffer> localBuffers = new ArrayList<LocalWalkBuffer>();

        synchronized void addLocalBuffer(LocalWalkBuffer buf) {
            localBuffers.add(buf);
        }

        /**
         * At the start of interval - grab the snapshot of walks
         */
        public void beginSubInterval(GraphChiContext ctx, final VertexInterval interval) {
            long t = System.currentTimeMillis();
            curWalkSnapshot = job.walkManager.grabSnapshot(interval.getFirstVertex(), interval.getLastVertex());
            logger.info("Grab snapshot took " + (System.currentTimeMillis() - t) + " ms.");

            while(localBuffers.size() > 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                logger.fine("Waiting for purge to finish...");
            }
        }

        public void endSubInterval(GraphChiContext ctx, final VertexInterval interval) {
            curWalkSnapshot.restoreUngrabbed();
            curWalkSnapshot = null; // Release memory

            /* Purge local buffers */
            Thread t = new Thread(new Runnable() {
                public void run() {
                    synchronized (localBuffers) {
                        final TimerContext _timer = purgeTimer.time();
                        for (LocalWalkBuffer buf : localBuffers) {
                            buf.purge(job.walkManager);
                        }
                        localBuffers.clear();
                        _timer.stop();
                    }
                }});
            t.start();
        }



        public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
            /* Count walks */
            long initializedWalks = job.walkManager.getTotalWalks();
            long activeWalks = job.walkManager.getNumOfActiveWalks();

            System.out.println("=====================================");
            System.out.println("Active walks: " + activeWalks + ", initialized=" + initializedWalks);
            System.out.println("=====================================");

            job.walkManager.populateSchedulerForInterval(ctx.getScheduler(), interval);
            job.walkManager.setBucketConsumer(this);
        }

        public void endInterval(GraphChiContext ctx, VertexInterval interval) {}

        public void consume(int firstVertexInBucket, int[] walkBucket, int len) {
            try {
                pendingWalksToSubmit.addAndGet(len);
                bucketQueue.put(new BucketsToSend(firstVertexInBucket, walkBucket, len));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private static class BucketsToSend {
        int firstVertex;
        int[] walks;
        int length;

        BucketsToSend(int firstVertex, int[] walks, int length) {
            this.firstVertex = firstVertex;
            this.walks = walks;
            this.length = length;
        }
    }

    /**
     * Encapsulates a random walk computation.
     */
    public class DrunkardJob {
        private String name;
        private WalkManager walkManager;
        private RemoteDrunkardCompanion companion;

        public DrunkardJob(String name,  RemoteDrunkardCompanion companion) {
            this.name = name;
            this.walkManager = walkManager;
            this.companion = companion;
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

    }

    static protected class LocalWalkBuffer {
        int[] walkBufferDests;
        int[] walkSourcesAndHops;
        Random random = new Random();

        int idx = 0;
        LocalWalkBuffer() {
            walkBufferDests = new int[65536];
            walkSourcesAndHops = new int[65536];
        }

        private void add(int src, int dst, boolean hop) {
            if (idx == walkSourcesAndHops.length) {
                int[] tmp = walkSourcesAndHops;
                walkSourcesAndHops = new int[tmp.length * 2];
                System.arraycopy(tmp, 0, walkSourcesAndHops, 0, tmp.length);

                tmp = walkBufferDests;
                walkBufferDests = new int[tmp.length * 2];
                System.arraycopy(tmp, 0, walkBufferDests, 0, tmp.length);
            }
            walkBufferDests[idx] = dst;
            walkSourcesAndHops[idx] = (hop ? -1 : 1) * (1 + src); // Note +1 so zero will be handled correctly
            idx++;
        }

        private void purge(WalkManager walkManager) {
            for(int i=0; i<idx; i++) {
                int dst = walkBufferDests[i];
                int src = walkSourcesAndHops[i];
                boolean hop = src < 0;
                if (src < 0) src = -src;
                src = src - 1;  // Note, -1
                walkManager.updateWalkUnsafe(src, dst, hop);
            }
            walkSourcesAndHops = null;
            walkBufferDests = null;
        }

    }
}
