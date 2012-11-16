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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Launch millions (?) of random walks and record the
 * hops for each source. Uses a remote DrunkardCompanion to
 * keep track of the distribution.
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
public class DrunkardMobWithCompanion implements GraphChiProgram<Integer, Boolean> {

    private WalkManager walkManager;
    private WalkSnapshot curWalkSnapshot;
    private RemoteDrunkardCompanion companion;
    private AtomicInteger outStanding = new AtomicInteger(0);

    public DrunkardMobWithCompanion(String companionAddress) throws Exception {
        companion = (RemoteDrunkardCompanion) Naming.lookup(companionAddress);
        System.out.println("Found companion: " + companion);

    }

    private void initCompanion() throws Exception {

        /* Tell companion the sources */
        companion.setSources(walkManager.getSources());
    }

    public void update(ChiVertex<Integer, Boolean> vertex, GraphChiContext context) {
        try {
            int[] walksAtMe = curWalkSnapshot.getWalksAtVertex(vertex.getId());
            if (context.getIteration() == 0) {
                vertex.setValue(0);

                if (walkManager.isSource(vertex.getId())) {
                    // If I am a source, tell the companion
                    int myIdx = walkManager.getVertexSourceIdx(vertex.getId());
                    // Add my out-neighbors to the avoidlist
                    companion.setAvoidList(myIdx, vertex.getOutNeighborArray());
                }
            }
            if (walksAtMe == null) return;

            int walkLength = walksAtMe.length;
            int numWalks = 0;
            for(int i=0; i < walkLength; i++) {
                int walk = walksAtMe[i];
                boolean hop = walkManager.hop(walk);
                // Choose a random destination and move the walk forward
                int dst;
                if (vertex.getId() != walkManager.getSourceVertex(walk)) {
                    numWalks++;
                }
                if (vertex.numOutEdges() > 0) {
                    dst = vertex.getRandomOutNeighbor();
                } else {
                    // Dead end!
                    dst = walkManager.getSourceVertex(walkManager.sourceIdx(walk));
                }
                walkManager.updateWalk(walkManager.sourceIdx(walk), dst, !hop);
                context.getScheduler().addTask(dst);

            }
            vertex.setValue(vertex.getValue() + numWalks);
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
        while (outStanding.get() > 0) {
            try {
                System.out.println("Waiting ...");
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * At the start of interval - grab the snapshot of walks
     */
    public void beginSubInterval(GraphChiContext ctx, final VertexInterval interval) {
        long t = System.currentTimeMillis();
        curWalkSnapshot = walkManager.grabSnapshot(interval.getFirstVertex(), interval.getLastVertex());
        System.out.println("Grab snapshot took " + (System.currentTimeMillis() - t) + " ms.");

        String walkDir = System.getProperty("walk.dir", ".");
        final String filename = walkDir + "/walks_" + interval.getFirstVertex() + "-" + interval.getLastVertex() + ".dat";
        if (ctx.getIteration() == 0) { // NOTE, temporary hack to save disk space but have the same I/O cost for testing
            new File(filename).delete();
        }

        final WalkSnapshot snapshot = curWalkSnapshot;
        outStanding.incrementAndGet();

        // Launch a thread to send to the companion
        Thread dumperThread = new Thread(new Runnable() {
            public void run() {
                try {
                    int n = snapshot.numWalks();
                    int[] walks = new int[n];
                    int[] vertices = new int[n];
                    int idx = 0;
                    for(int v=snapshot.getFirstVertex(); v<=snapshot.getLastVertex(); v++) {
                        int[] walksAtVertex = snapshot.getWalksAtVertex(v);
                        if (walksAtVertex != null) {
                            for(int j=0; j<walksAtVertex.length; j++) {
                                walks[idx] = walksAtVertex[j];
                                vertices[idx] = v;
                                idx++;
                            }
                        }
                    }
                    synchronized (companion) {
                        companion.processWalks(walks, vertices);
                    }
                } catch (Exception err) {
                    err.printStackTrace();
                }  finally {
                    outStanding.decrementAndGet();

                }
            }
        });
        dumperThread.start();
    }

    public void endSubInterval(GraphChiContext ctx, final VertexInterval interval) {
        curWalkSnapshot = null; // Release memory
    }

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}

    public static void main(String[] args) throws  Exception {

        SimpleMetricsReporter rep = SimpleMetricsReporter.enable(2, TimeUnit.MINUTES);

        String baseFilename = args[0];

        if (args.length > 1) {
            int nShards = Integer.parseInt(args[1]);
            int nSources = Integer.parseInt(args[2]);
            int walksPerSource = Integer.parseInt(args[3]);
            int maxHops = Integer.parseInt(args[4]);

            System.out.println("Walks will start from " + nSources + " sources.");
            System.out.println("Going to start " + walksPerSource + " walks per source.");
            System.out.println("Max hops: " + maxHops);

            /* Delete vertex data */
            File vertexDataFile = new File(ChiFilenames.getFilenameOfVertexData(baseFilename, new IntConverter()));
            if (vertexDataFile.exists()) {
                vertexDataFile.delete();
            }

            /* Initialize GraphChi engine */
            GraphChiEngine<Integer, Boolean> engine = new GraphChiEngine<Integer, Boolean>(baseFilename, nShards);
            engine.setEdataConverter(null);
            engine.setVertexDataConverter(new IntConverter());
            engine.setModifiesInedges(false);
            engine.setModifiesOutedges(false);
            engine.setEnableScheduler(true);
            engine.setOnlyAdjacency(true);
            engine.setDisableInedges(true);
            engine.setMemoryBudgetMb(1200);
            engine.setUseStaticWindowSize(false); // Disable dynamic window size detection
            engine.setEnableDeterministicExecution(false);
            engine.setAutoLoadNext(false);
            engine.setMaxWindow(2000000); // Handle maximum 2M vertices a time.

            long t1 = System.currentTimeMillis();

            /* Initialize application object */
            DrunkardMobWithCompanion mob = new DrunkardMobWithCompanion("//:1099/drunkarcompanion");

            /* Initialize Random walks */
            int nVertices = engine.numVertices();
            mob.walkManager = new WalkManager(nVertices, nSources);

            for(int i=0; i < nSources; i++) {
                int source = (int) (Math.random() * nVertices);
                mob.walkManager.addWalkBatch(source, walksPerSource);
            }
            mob.walkManager.initializeWalks();

            mob.initCompanion();

            System.out.println("Configured " + mob.walkManager.getTotalWalks() + " walks in " +
                    (System.currentTimeMillis() - t1) + " ms");


            /* Run */
            engine.run(mob, maxHops + 1);

            // TODO: ensure that we have sent all walks!
            mob.spinUntilFinish();
            mob.companion.outputDistributions(baseFilename + "_distribution.tsv");
        }
        System.out.println("Ready. Going to output...");

        TreeSet<IdInt> top20 = Toplist.topListInt(baseFilename, 20);
        int i = 0;
        for(IdInt vertexRank : top20) {
            System.out.println(++i + ": " + vertexRank.getVertexId() + " = " + vertexRank.getValue());
        }
        System.out.println("Finished.");
        long sumWalks = VertexAggregator.sumInt(baseFilename);
        System.out.println("Total hops (in file): " + sumWalks);
        rep.run();
    }
}
