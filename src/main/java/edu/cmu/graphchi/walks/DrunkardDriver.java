package edu.cmu.graphchi.walks;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.engine.VertexInterval;

/**
 * Class to encapsulate the graphchi program running the show.
 * Due to several optimizations, it is quite complicated!
 */
public abstract class DrunkardDriver<VertexDataType, EdgeDataType> implements GrabbedBucketConsumer {
    private WalkSnapshot curWalkSnapshot;
    protected final DrunkardJob job;
    protected static Logger logger = ChiLogger.getLogger("drunkard-driver");

    protected LinkedBlockingQueue<BucketsToSend> bucketQueue = new LinkedBlockingQueue<BucketsToSend>();
    protected AtomicBoolean finished = new AtomicBoolean(false);
    protected AtomicLong pendingWalksToSubmit = new AtomicLong(0);
    private Thread dumperThread;
    WalkUpdateFunction<VertexDataType, EdgeDataType> callback;

    private final Timer purgeTimer =
            Metrics.defaultRegistry().newTimer(DrunkardMobEngine.class, "purge-localwalks", TimeUnit.SECONDS, TimeUnit.MINUTES);


    DrunkardDriver(final DrunkardJob job, WalkUpdateFunction<VertexDataType, EdgeDataType> callback) {
        this.job = job;
        this.callback = callback;

        // Setup thread for sending walks to the companion (i.e tracker)
        // Launch a thread to send to the companion
        dumperThread = new Thread(createDumperThread());
        dumperThread.start();
    }

    protected abstract DumperThread createDumperThread();

    public DrunkardJob getJob() {
        return job;
    }

    protected abstract DrunkardContext createDrunkardContext(int vertexId, GraphChiContext context,
            LocalWalkBuffer localBuf);

    public void update(ChiVertex<VertexDataType, EdgeDataType> vertex,
            final GraphChiContext context, final LocalWalkBuffer localBuf) {
        try {
            // Flow control
            while (pendingWalksToSubmit.get() > job.getWalkManager().getTotalWalks() / 40) {
                //System.out.println("Too many walks waiting for delivery: " + pendingWalksToSubmit.get());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            }

            boolean  firstIteration = (context.getIteration() == 0);
            WalkArray walksAtMe = curWalkSnapshot.getWalksAtVertex(vertex.getId(), true);

            // Very dirty memory management
            curWalkSnapshot.clear(vertex.getId());

            // On first iteration, we ask the callback for list of vertices
            // that should not be tracked
            if (firstIteration) {
                if (job.getWalkManager().isSource(vertex.getId())) {
                    int mySourceIdx = job.getWalkManager().getVertexSourceIdx(vertex.getId());

                    job.getCompanion().setAvoidList(mySourceIdx, callback.getNotTrackedVertices(vertex));
                }
            }
            if (walksAtMe == null || walksAtMe.size() == 0) return;

            Random randomGenerator = localBuf.random;

            DrunkardContext drunkardContext = createDrunkardContext(vertex.getId(), context, localBuf);
            callback.processWalksAtVertex(walksAtMe, vertex, drunkardContext, randomGenerator);
        } catch (RemoteException re) {
            throw new RuntimeException(re);
        }
    }

    public void initWalks() throws RemoteException{
        job.getWalkManager().initializeWalks();
        job.getCompanion().setSources(job.getWalkManager().getSources());
    }

    public void beginIteration(GraphChiContext ctx) {
        if (ctx.getIteration() == 0) {
            ctx.getScheduler().removeAllTasks();
            job.getWalkManager().populateSchedulerWithSources(ctx.getScheduler());
        }
    }

    public void endIteration(GraphChiContext ctx) {}

    public void spinUntilFinish() {
        finished.set(true);
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
        curWalkSnapshot = job.getWalkManager().grabSnapshot(interval.getFirstVertex(), interval.getLastVertex());
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
        synchronized (localBuffers) {
            final TimerContext _timer = purgeTimer.time();
            for (LocalWalkBuffer buf : localBuffers) {
                buf.purge(job.getWalkManager());
            }
            localBuffers.clear();
            _timer.stop();
        }
    }

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
        /* Count walks */
        long initializedWalks = job.getWalkManager().getTotalWalks();
        long activeWalks = job.getWalkManager().getNumOfActiveWalks();

        System.out.println("=====================================");
        System.out.println("Active walks: " + activeWalks + ", initialized=" + initializedWalks);
        System.out.println("=====================================");

        job.getWalkManager().populateSchedulerForInterval(ctx.getScheduler(), interval);
        job.getWalkManager().setBucketConsumer(this);
    }

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void consume(int firstVertexInBucket, WalkArray walkBucket, int len) {
        try {
            pendingWalksToSubmit.addAndGet(len);
            bucketQueue.put(new BucketsToSend(firstVertexInBucket, walkBucket, len));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
