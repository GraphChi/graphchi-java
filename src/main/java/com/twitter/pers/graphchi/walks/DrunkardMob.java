package com.twitter.pers.graphchi.walks;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.aggregators.VertexAggregator;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.util.IdInt;
import edu.cmu.graphchi.util.Toplist;

import java.io.File;
import java.util.TreeSet;

/**
 * Launch millions (?) of random walks and record the
 * distribution.
 */
public class DrunkardMob implements GraphChiProgram<Integer, Boolean> {

    private WalkManager walkManager;
    private WalkSnapshot curWalkSnapshot;
    private int maxHops;
    private String basefileName;

    public DrunkardMob(int maxHops, String basefileName) {
        this.maxHops = maxHops;
        this.basefileName = basefileName;
    }

    public void update(ChiVertex<Integer, Boolean> vertex, GraphChiContext context) {
        int[] walksAtMe = curWalkSnapshot.getWalksAtVertex(vertex.getId());
        if (context.getIteration() == 0) vertex.setValue(0);
        if (walksAtMe == null) return;

        int walkLength = WalkManager.getWalkLength(walksAtMe);
        for(int i=0; i < walkLength; i++) {
            int walk = walksAtMe[i];
            int hop = walkManager.hop(walk);
            if (hop < maxHops) {
                // Choose a random destination and move the walk forward
                int dst;
                if (vertex.numOutEdges() > 0) {
                    dst = vertex.getRandomOutNeighbor();
                } else {
                    // Dead end!
                    dst = walkManager.getSourceVertex(walkManager.sourceIdx(walk));
                }
                walkManager.updateWalk(walkManager.sourceIdx(walk), dst, hop + 1);
                context.getScheduler().addTask(dst);
            }
        }
        if (context.getIteration() > 0)
            vertex.setValue(vertex.getValue() + walkLength);
    }


    public void beginIteration(GraphChiContext ctx) {}

    public void endIteration(GraphChiContext ctx) {}

    /**
     * At the start of interval - grab the snapshot of walks
     */
    public void beginSubInterval(GraphChiContext ctx, final VertexInterval interval) {
        long t = System.currentTimeMillis();
        curWalkSnapshot = walkManager.grabSnapshot(interval.getFirstVertex(), interval.getLastVertex());
        System.out.println("Grab snapshot took " + (System.currentTimeMillis() - t) + " ms.");

        final String filename = basefileName + "_walks_" + interval.getFirstVertex() + "-" + interval.getLastVertex() + ".csv";
        if (ctx.getIteration() == 0) {
            new File(filename).delete();
        }

        // Launch a thread to dump
        Thread dumperThread = new Thread(new Runnable() {
            public void run() {
                try {
                    walkManager.dumpToFile(curWalkSnapshot, filename);
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }
        });
        dumperThread.start();
    }

    public void endSubInterval(GraphChiContext ctx, final VertexInterval interval) {}

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}

    public static void main(String[] args) throws  Exception {
        String baseFilename = args[0];
        int nShards = Integer.parseInt(args[1]);
        int nSources = Integer.parseInt(args[2]);
        int walksPerSource = Integer.parseInt(args[3]);
        int maxHops = Integer.parseInt(args[4]);

        System.out.println("Walks will start from " + nSources + " sources.");
        System.out.println("Going to start " + walksPerSource + " walks per source.");
        System.out.println("Max hops: " + maxHops);

        /* Initialize GraphChi engine */
        GraphChiEngine<Integer, Boolean> engine = new GraphChiEngine<Integer, Boolean>(baseFilename, nShards);
        engine.setEdataConverter(null);
        engine.setVertexDataConverter(new IntConverter());
        engine.setModifiesInedges(false);
        engine.setModifiesOutedges(false);
        engine.setEnableScheduler(true);
        engine.setOnlyAdjacency(true);
        engine.setDisableInedges(true);
        engine.setUseStaticWindowSize(true); // Disable dynamic window size detection
        engine.setEnableDeterministicExecution(false);
        engine.setMaxWindow(1000000); // Handle maximum 1M vertices a time.

        long t1 = System.currentTimeMillis();

        /* Initialize application object */
        DrunkardMob mob = new DrunkardMob(maxHops, baseFilename);

        /* Initialize Random walks */
        int nVertices = engine.numVertices();
        mob.walkManager = new WalkManager(nVertices);

        for(int i=0; i < nSources; i++) {
            int source = (int) (Math.random() * nVertices);
            mob.walkManager.addWalkBatch(source, walksPerSource);
        }
        mob.walkManager.initializeWalks();

        System.out.println("Configured " + mob.walkManager.getTotalWalks() + " walks in " +
                (System.currentTimeMillis() - t1) + " ms");


        /* Run */
        engine.run(mob, 1 + maxHops);

        System.out.println("Ready. Going to output...");

        TreeSet<IdInt> top20 = Toplist.topListInt(baseFilename, 20);
        int i = 0;
        for(IdInt vertexRank : top20) {
            System.out.println(++i + ": " + vertexRank.getVertexId() + " = " + vertexRank.getValue());
        }
        System.out.println("Finished.");

        long sumWalks = VertexAggregator.sumInt(baseFilename);
        System.out.println("Total hops (in file): " + sumWalks);
    }
}
