package com.twitter.pers.shortestpath;

import com.twitter.pers.multicomp.ComputationInfo;
import com.yammer.metrics.Metrics;
import edu.cmu.graphchi.ChiEdge;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.metrics.SimpleMetricsReporter;
import edu.cmu.graphchi.util.HugeLongMatrix;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *  @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
public class SketchBasedDistance implements GraphChiProgram<Integer, Integer> {

    private final static int IN_DISTANCES = 0;
    private final static int OUT_DISTANCES = 1;
    private final static int SEEDSETS = 9;

    SketchSet sketchSet;
    private HugeLongMatrix distanceMatrix;


    public SketchBasedDistance(String graphName, int numVertices) throws IOException {
        sketchSet = new SketchSet(SEEDSETS);
        sketchSet.selectSeeds(graphName, numVertices);
        distanceMatrix = new HugeLongMatrix(numVertices, 2);

        /* Initialize seeds */
        for(int seedSet=0; seedSet<SEEDSETS; seedSet++) {
            int[] seedVertices = sketchSet.seeds(seedSet);
            for(int seedIdx=0; seedIdx < seedVertices.length; seedIdx++) {
                int v = seedVertices[seedIdx];
                distanceMatrix.setValue(v, IN_DISTANCES, sketchSet.encode(distanceMatrix.getValue(v, IN_DISTANCES), seedSet, seedIdx, 0));
                distanceMatrix.setValue(v, OUT_DISTANCES, sketchSet.encode(distanceMatrix.getValue(v, OUT_DISTANCES), seedSet, seedIdx, 0));

                System.out.println("Seed " + seedSet + " / " + v);
            }
        }
    }

    @Override
    public void update(ChiVertex<Integer, Integer> vertex, GraphChiContext context) {
        /* Maintain shortest seed to both directions */
        for(int direction=IN_DISTANCES; direction <= OUT_DISTANCES; direction++) {
            long myDistances = distanceMatrix.getValue(vertex.getId(), direction);
            long origDistances = myDistances;

            int nEdges = (direction == IN_DISTANCES ? vertex.numInEdges() : vertex.numOutEdges());
            for(int i=0; i<nEdges; i++) {
                ChiEdge<Integer> edge = (direction == IN_DISTANCES ? vertex.inEdge(i) : vertex.outEdge(i));
                long nbDistances = distanceMatrix.getValue(edge.getVertexId(), direction);
                for(int s=0; s<SEEDSETS; s++) {
                    int nbDistance = sketchSet.distance(nbDistances, s);
                    int myDistance = sketchSet.distance(myDistances, s);
                    if (nbDistance + 1 < myDistance) {
                        // My distance is min(1 + shortest distance to a neighbor )
                        myDistances = sketchSet.encode(myDistances, s, sketchSet.seedIndex(nbDistances, s), nbDistance + 1);
                    }
                }
            }

            /* Set the new value */
            if (origDistances != myDistances) {
                distanceMatrix.setValue(vertex.getId(), direction, myDistances);

                // Schedule appropriate neighbors
                if (direction == IN_DISTANCES) {
                    context.getScheduler().scheduleOutNeighbors(vertex);
                } else {
                    context.getScheduler().scheduleInNeighbors(vertex);

                }
            }

        }
    }

    public void beginIteration(GraphChiContext ctx) {
    }

    public void endIteration(GraphChiContext ctx) {
    }

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public static void main(String[] args) throws  Exception {
        SimpleMetricsReporter rep = SimpleMetricsReporter.enable(2, TimeUnit.MINUTES);

        int k=0;
        String graph = args[k++];
        int nshards = Integer.parseInt(args[k++]);
        int niters = Integer.parseInt(args[k++]);

        GraphChiEngine<Integer, Integer> engine = new GraphChiEngine<Integer, Integer>(graph, nshards);
        SketchBasedDistance sketchApp = new SketchBasedDistance(graph, engine.numVertices());

        engine.setOnlyAdjacency(true);
        engine.setAutoLoadNext(true);
        engine.setModifiesInedges(false);
        engine.setModifiesOutedges(false);
        engine.setEnableDeterministicExecution(true);
        engine.setEdataConverter(null);
        engine.setVertexDataConverter(null);
        engine.setEnableScheduler(true);
        engine.run(sketchApp, niters);

        sketchApp.save(graph + ".distancemat");

        /* Report metrics */
        Metrics.shutdown();
        rep.run();
    }

    private void save(String outputFile) throws IOException {
        System.out.println("Saving .... " + outputFile);
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));

        // Write seeds
        dos.writeInt(SEEDSETS);
        /* Initialize seeds */
        for(int seedSet=0; seedSet<SEEDSETS; seedSet++) {
            int[] seedVertices = sketchSet.seeds(seedSet);
            dos.writeInt(seedVertices.length);
            for(int seedIdx=0; seedIdx < seedVertices.length; seedIdx++) {
                int v = seedVertices[seedIdx];
                dos.writeInt(v);
            }
        }
        int nVertices = (int) distanceMatrix.getNumRows();

        dos.writeInt(nVertices);
        for(int i=0; i <nVertices; i++) {
            long inDist = distanceMatrix.getValue(i, IN_DISTANCES);
            long outDist = distanceMatrix.getValue(i, OUT_DISTANCES);
            dos.writeLong(inDist);
            dos.writeLong(outDist);
        }
        dos.close();
        System.out.println("Done");
    }


}
