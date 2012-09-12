package com.twitter.pers.graphchi.walks;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;

import java.io.File;

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
        for(int i=0; i < walksAtMe.length; i++) {
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

    }


    public void beginIteration(GraphChiContext ctx) {}

    public void endIteration(GraphChiContext ctx) {}

    /**
     * At the start of interval - grab the snapshot of walks
     */
    public void beginInterval(GraphChiContext ctx, final VertexInterval interval) {
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

        System.out.println("Gonfigured " + mob.walkManager.getTotalWalks() + " walks in " +
                (System.currentTimeMillis() - t1) + " ms");


        /* Run */
        engine.run(mob, 1 + maxHops);

        System.out.println("Ready. Going to output...");
        System.out.println("Finished. See file: " + baseFilename + ".walks");
    }
}
