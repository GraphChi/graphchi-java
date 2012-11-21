package com.twitter.pers.graphchi.walks;

import com.twitter.pers.graphchi.walks.distributions.RemoteDrunkardCompanion;
import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.aggregators.VertexAggregator;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.metrics.SimpleMetricsReporter;
import edu.cmu.graphchi.util.IdInt;
import edu.cmu.graphchi.util.Toplist;

import java.io.File;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Launch millions (?) of random walks and record the
 * hops for each source. Uses a remote DrunkardCompanion to
 * keep track of the distribution.
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
public class DrunkardMobWithCompanion implements GraphChiProgram<Integer, Boolean>, GrabbedBucketConsumer {

    private WalkManager walkManager;
    private WalkSnapshot curWalkSnapshot;
    private final RemoteDrunkardCompanion companion;

    private final static double RESETPROB = 0.15;
    private LinkedBlockingQueue<BucketsToSend> bucketQueue = new LinkedBlockingQueue<BucketsToSend>();
    private boolean finished = false;
    private Thread dumperThread;

    private AtomicLong pendingWalksToSubmit = new AtomicLong(0);

    public DrunkardMobWithCompanion(String companionAddress) throws Exception {
        if (companionAddress.contains("localhost")) {
            RMIHack.setupLocalHostTunneling();
        }
        companion = (RemoteDrunkardCompanion) Naming.lookup(companionAddress);
        System.out.println("Found companion: " + companion);

        // Launch a thread to send to the companion
        dumperThread = new Thread(new Runnable() {
            public void run() {
                int[] walks = new int[256 * 1024];
                int[] vertices = new int[256 * 1024];
                int idx = 0;
                long ignoreCount = 0;
                long counter = 0;

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

                            boolean atleastSecondHop = WalkManager.hop(w);

                            if (!atleastSecondHop) {
                                ignoreCount++;
                                continue;
                            }

                            walks[idx] = w;
                            vertices[idx] = v;
                            idx++;

                            if (idx >= walks.length) {
                                try {
                                    companion.processWalks(walks, vertices);
                                } catch (Exception err) {
                                    err.printStackTrace();
                                }
                                idx = 0;
                            }

                        }
                        if (counter++ % 100 == 0)
                            System.out.println("Ignore count:" + ignoreCount + "; pending=" + pendingWalksToSubmit.get());
                    }
                }

                // Send rest
                try {
                    int[] tmpwalks = new int[idx];
                    int[] tmpvertices = new int[idx];
                    System.arraycopy(walks, 0, tmpwalks, 0, idx);
                    System.arraycopy(vertices, 0, tmpvertices, 0, idx);
                    companion.processWalks(tmpwalks, tmpvertices);
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }
        });
        dumperThread.start();
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

    @Override
    public void consume(int firstVertexInBucket, int[] walkBucket, int len) {
        try {
            pendingWalksToSubmit.addAndGet(len);
            bucketQueue.put(new BucketsToSend(firstVertexInBucket, walkBucket, len));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void initCompanion() throws Exception {

        /* Tell companion the sources */
        companion.setSources(walkManager.getSources());
    }

    public void update(ChiVertex<Integer, Boolean> vertex, GraphChiContext context) {
        try {
            // Flow control
            while (pendingWalksToSubmit.get() > walkManager.getTotalWalks() / 40) {
                System.out.println("Too many walks waiting for delivery: " + pendingWalksToSubmit.get());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                }
            }

            boolean  firstIteration = (context.getIteration() == 0);
            int[] walksAtMe = curWalkSnapshot.getWalksAtVertex(vertex.getId(), true);

            // Very dirty memory managenet
            curWalkSnapshot.clear(vertex.getId());

            int mySourceIdx = -1;
            if (walkManager.isSource(vertex.getId())) {
                // If I am a source, tell the companion
                mySourceIdx = walkManager.getVertexSourceIdx(vertex.getId());
                // Add my out-neighbors to the avoidlist. TODO: async
                if (firstIteration) {
                    companion.setAvoidList(mySourceIdx, vertex.getOutNeighborArray());
                }
            }

            if (walksAtMe == null) return;

            int walkLength = walksAtMe.length;
            for(int i=0; i < walkLength; i++) {
                int walk = walksAtMe[i];

                boolean atleastSecondHop = WalkManager.hop(walk);

                if (!atleastSecondHop) {
                    if (WalkManager.sourceIdx(walk) == mySourceIdx) {
                        atleastSecondHop = false;
                    } else {
                        atleastSecondHop = true;
                    }
                }

                // Choose a random destination and move the walk forward, or
                // reset (not on first iteration).
                int dst;
                if (vertex.numOutEdges() > 0 && (Math.random() > RESETPROB || firstIteration)) {
                    dst = vertex.getRandomOutNeighbor();
                } else {
                    // Dead end or reset
                    dst = walkManager.getSourceVertex(WalkManager.sourceIdx(walk));
                }
                walkManager.updateWalk(WalkManager.sourceIdx(walk), dst, atleastSecondHop);
            }
        } catch (RemoteException re) {
            throw new RuntimeException(re);
        }
    }


    public void beginIteration(GraphChiContext ctx) {
        if (ctx.getIteration() == 0) {
            ctx.getScheduler().removeAllTasks();
            walkManager.populateSchedulerWithSources(ctx.getScheduler());
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

    /**
     * At the start of interval - grab the snapshot of walks
     */
    public void beginSubInterval(GraphChiContext ctx, final VertexInterval interval) {
        long t = System.currentTimeMillis();
        curWalkSnapshot = walkManager.grabSnapshot(interval.getFirstVertex(), interval.getLastVertex());
        System.out.println("Grab snapshot took " + (System.currentTimeMillis() - t) + " ms.");
    }

    public void endSubInterval(GraphChiContext ctx, final VertexInterval interval) {
        curWalkSnapshot.restoreUngrabbed();
        curWalkSnapshot = null; // Release memory
    }

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
        walkManager.populateSchedulerForInterval(ctx.getScheduler(), interval);
        walkManager.setBucketConsumer(this);
    }

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}

    public static void main(String[] args) throws  Exception {

        SimpleMetricsReporter rep = SimpleMetricsReporter.enable(2, TimeUnit.MINUTES);

        String baseFilename = args[0];

        if (args.length > 1) {
            int nShards = Integer.parseInt(args[1]);
            int nSources = Integer.parseInt(args[2]);
            int walksPerSource = Integer.parseInt(args[3]);
            int maxHops = Integer.parseInt(args[4]);
            int firstSource = Integer.parseInt(args[5]);
            String companionAddress = args[6];

            System.out.println("Walks will start from vertices " + firstSource + " -- " + (firstSource + nSources - 1) );
            System.out.println("Going to start " + walksPerSource + " walks per source.");
            System.out.println("Max hops: " + maxHops);
            System.out.println("Companion: " + companionAddress);

            /* Delete vertex data */
            File vertexDataFile = new File(ChiFilenames.getFilenameOfVertexData(baseFilename, new IntConverter()));
            if (vertexDataFile.exists()) {
                vertexDataFile.delete();
            }

            /* Initialize GraphChi engine */
            GraphChiEngine<Integer, Boolean> engine = new GraphChiEngine<Integer, Boolean>(baseFilename, nShards);
            engine.setEdataConverter(null);
            engine.setModifiesInedges(false);
            engine.setModifiesOutedges(false);
            engine.setEnableScheduler(true);
            engine.setOnlyAdjacency(true);
            engine.setDisableInedges(true);

            int memoryBudget = 1200;
            if (System.getProperty("membudget") != null) memoryBudget = Integer.parseInt(System.getProperty("membudget"));

            System.out.println("Memory budget: " + memoryBudget);
            engine.setMemoryBudgetMb(memoryBudget);
            engine.setEnableDeterministicExecution(false);
            engine.setAutoLoadNext(false);
            engine.setVertexDataConverter(null);
            engine.setMaxWindow(10000000); // Handle maximum 10M vertices a time.

            long t1 = System.currentTimeMillis();

            /* Initialize application object */
            DrunkardMobWithCompanion mob = new DrunkardMobWithCompanion(companionAddress);

            /* Initialize Random walks */
            int nVertices = engine.numVertices();
            mob.walkManager = new WalkManager(nVertices, nSources);

            for(int i=0; i < nSources; i++) {
                mob.walkManager.addWalkBatch(i + firstSource, walksPerSource);
                if (i % 100000 == 0) System.out.println("Add walk batch: " + (i + firstSource));
            }

            System.out.println("Initializing walks...");
            mob.walkManager.initializeWalks();

            mob.initCompanion();

            System.out.println("Configured " + mob.walkManager.getTotalWalks() + " walks in " +
                    (System.currentTimeMillis() - t1) + " ms");


            /* Run */
            engine.run(mob, maxHops + 1);

            // TODO: ensure that we have sent all walks!
            mob.spinUntilFinish();
            mob.companion.outputDistributions(baseFilename + "_" + firstSource);
        }

        rep.run();
    }
}
