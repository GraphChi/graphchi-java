package edu.cmu.graphchi.walks;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import edu.cmu.graphchi.*;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Class to encapsulate the graphchi program running the show.
 * Due to several optimizations, it is quite complicated!
 */
public class DrunkardDriver<VertexDataType, EdgeDataType> implements GrabbedBucketConsumer {
    private WalkSnapshot curWalkSnapshot;
    private final DrunkardJob job;
    private static Logger logger = ChiLogger.getLogger("drunkard-driver");

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
                                    job.getCompanion().processWalks(walks, vertices);
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
                    job.getCompanion().processWalks(tmpWalks, tmpVertices);
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }
        });
        dumperThread.start();
    }

    public DrunkardJob getJob() {
        return job;
    }

    public void update(ChiVertex<VertexDataType, EdgeDataType> vertex, final GraphChiContext context,
                       final LocalWalkBuffer localBuf) {


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
            int[] walksAtMe = curWalkSnapshot.getWalksAtVertex(vertex.getId(), true);

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
            if (walksAtMe == null || walksAtMe.length == 0)  {
                return;
            }

            Random randomGenerator = localBuf.random;

            final boolean  isSource = job.getWalkManager().isSource(vertex.getId());
            final int mySourceIndex = (isSource ? job.getWalkManager().getVertexSourceIdx(vertex.getId()) : -1);

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
                    forwardWalkTo(walk, job.getWalkManager().getSourceVertex(WalkManager.sourceIdx(walk)), false);
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

                @Override
                public void resetAll(int[] walks) {
                    for(int w : walks) resetWalk(w, false);
                }
            }, randomGenerator);
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

    public void consume(int firstVertexInBucket, int[] walkBucket, int len) {
        try {
            pendingWalksToSubmit.addAndGet(len);
            bucketQueue.put(new BucketsToSend(firstVertexInBucket, walkBucket, len));
        } catch (InterruptedException e) {
            e.printStackTrace();
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

}

