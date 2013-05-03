package edu.cmu.graphchi.apps.recommendations;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.EdgeDirection;
import edu.cmu.graphchi.apps.ALSMatrixFactorization;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.util.IdCount;
import edu.cmu.graphchi.walks.DrunkardContext;
import edu.cmu.graphchi.walks.DrunkardJob;
import edu.cmu.graphchi.walks.DrunkardMobEngine;
import edu.cmu.graphchi.walks.WalkUpdateFunction;
import edu.cmu.graphchi.walks.WeightedHopper;
import edu.cmu.graphchi.walks.distributions.DrunkardCompanion;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Uses Netflix ratings data (or equivalent data) to compute recommendations
 * for users. Computes in three steps:
 *   1. Runs Alternating Least Squares (ALS) matrix factorization to learn a model to predict ratings for user, movie pairs.
 *   2. Uses random walks to find candidate movies
 *   3. Computes predicted ratings to the candidate movies and returns the best ones.
 * @author Aapo Kyrola
 */
public class MovieRecommender {

    protected String baseFilename;
    protected int nShards;
    protected int D;
    protected static Logger logger = ChiLogger.getLogger("movie-recommender");


    public MovieRecommender(String baseFilename, int nShards, int D) {
        this.baseFilename = baseFilename;
        this.nShards = nShards;
        this.D = D;
    }

    protected void execute() throws Exception {
        /* Step 1. Compute ALS */
        ALSMatrixFactorization als = ALSMatrixFactorization.computeALS(baseFilename, nShards, D);

        logger.info("Computed ALS, now random walks");

        /* Initialize drunkardmob */
        DrunkardMobEngine<Integer, Float> drunkardMobEngine = new DrunkardMobEngine<Integer, Float>(baseFilename, nShards);
        DrunkardJob positiveJob = drunkardMobEngine.addJob("positive", EdgeDirection.IN_AND_OUT_EDGES,
                new PositiveWalkUpdate(), new DrunkardCompanion(2, Runtime.getRuntime().maxMemory() / 8));
        DrunkardJob negativeJob = drunkardMobEngine.addJob("negative", EdgeDirection.IN_AND_OUT_EDGES,
                new NegativeWalkUpdate(), new DrunkardCompanion(2, Runtime.getRuntime().maxMemory() / 8));

        drunkardMobEngine.setEdataConverter(new FloatConverter());

        /* Create list of user vertices (i.e vertices on left). But we need to find their internal ids. */
        ALSMatrixFactorization.BipartiteGraphInfo graphInfo = als.getGraphInfo();
        VertexIdTranslate vertexIdTranslate = drunkardMobEngine.getVertexIdTranslate();
        ArrayList<Integer> userVertices = new ArrayList<Integer>(graphInfo.getNumLeft());

        int numUsers = 50000; // NOTE: hard-coded
        int walksPerSource = 6000;

        if (numUsers > graphInfo.getNumLeft())  graphInfo.getNumLeft();
        logger.info("Compute predictions for first " + numUsers + " users");

        for(int i=0; i< numUsers; i++) {
            userVertices.add(vertexIdTranslate.forward(i));
        }

        /* Configure */
        positiveJob.configureWalkSources(userVertices, walksPerSource);
        negativeJob.configureWalkSources(userVertices, walksPerSource);

        /* Run */
        drunkardMobEngine.run(6);

        /* TODO: handle results */
        for(int i=0; i< 500; i++) {
            int userId = vertexIdTranslate.forward(i);
            IdCount[] posTop = positiveJob.getCompanion().getTop(userId, 40);
            IdCount[] negTop =  negativeJob.getCompanion().getTop(userId, 40);

            for(int j=0; j<Math.min(posTop.length, negTop.length); j++) {
                System.out.println(i + " " + j + " +" + posTop[j].id + ":" + posTop[j].count + ":" + als.predict(userId, posTop[j].id)
                        + "    -" + negTop[j].id + ":" + negTop[j].count + ":" + als.predict(userId, negTop[j].id));
            }
        }
    }

    /* Positive update follows only 4 and 5 rated movies from the beginning */
    protected static class PositiveWalkUpdate implements WalkUpdateFunction<Integer, Float> {

        @Override
        public void processWalksAtVertex(int[] walks, ChiVertex<Integer, Float> vertex, DrunkardContext drunkardContext, Random randomGenerator) {
            hopToHighRatings(walks, vertex, drunkardContext, randomGenerator);
        }

        protected static void hopToHighRatings(int[] walks, ChiVertex<Integer, Float> vertex, DrunkardContext drunkardContext, Random randomGenerator) {
            int[] hops = WeightedHopper.generateRandomHopsAliasMethod(randomGenerator, vertex, walks.length, EdgeDirection.IN_AND_OUT_EDGES,
                    new WeightedHopper.EdgeWeightMap() {
                        // Use exponential weights
                        @Override
                        public float map(float x) {
                            int r = (int) x;
                            return (float) (1 << r); // 2^(rating - 1)     // TODO: should just eliminate negative?
                        }
                    });
            for(int i=0; i < walks.length; i++) {
                // Track only movie vertices
                drunkardContext.forwardWalkTo(walks[i],
                        vertex.edge(hops[i]).getVertexId(), vertex.numOutEdges() > 0);
            }
        }

        @Override
        /**
         * Do not track the vertex itself and its immediate neighbors
         */
        public int[] getNotTrackedVertices(ChiVertex<Integer, Float> vertex) {
            int[] notCounted = new int[1 + vertex.numOutEdges()];
            for(int i=0; i < vertex.numOutEdges(); i++) {
                notCounted[i + 1] = vertex.getOutEdgeId(i);
            }
            notCounted[0] = vertex.getId();
            return notCounted;
        }
    }

    /* Negative update follows first only 1 and 2 rated movies and then starts following
     * well-rated movies. Idea is to jump to users who have different taste and then
     * find what they like.   TODO: this is way to complicated, due to the asynchronous nature of drunkardmob!
    */

    protected class NegativeWalkUpdate extends PositiveWalkUpdate {
        @Override
        public void processWalksAtVertex(int[] walks, ChiVertex<Integer, Float> vertex, DrunkardContext drunkardContext, Random randomGenerator) {
            // Movie vertex - do same as the positive
            if (vertex.numInEdges() > 0 || drunkardContext.getIteration() > 0) {
                hopToHighRatings(walks, vertex, drunkardContext, randomGenerator);
            } else {
                // First: if there are already walks in this vertex (due to async nature -- CLARIFY), make a separate
                // list of them
                ArrayList<Integer> forwardToPositive = new ArrayList<Integer>();
                for(int w : walks) {
                    if (!drunkardContext.isWalkStartedFromVertex(w)) {
                        forwardToPositive.add(w);
                    }
                }
                if (forwardToPositive.size() > 0) {
                    int[] fwd = new int[forwardToPositive.size()];
                    for(int i=0; i<fwd.length; i++) fwd[i] = forwardToPositive.get(i);
                    hopToHighRatings(fwd, vertex, drunkardContext, randomGenerator);
                }

                /* Then, handle the negative cases */
                ArrayList<Integer> badlyRated = new ArrayList<Integer>();
                for(int i=0; i<vertex.numOutEdges(); i++) {
                    if (vertex.getOutEdgeValue(i) < 3) {
                        badlyRated.add(vertex.getOutEdgeId(i));
                    }
                }

                if (badlyRated.size() == 0) {
                    logger.info("No badly rated movies for user " + drunkardContext.getVertexIdTranslate().backward(vertex.getId()));
                    // No can do -- so no negative walks from this vertex
                    return;
                }

                for(int w : walks) {
                    if (drunkardContext.isWalkStartedFromVertex(w)) {
                        int randomBadRating = badlyRated.get(randomGenerator.nextInt(badlyRated.size()));
                        drunkardContext.forwardWalkTo(w, randomBadRating, true);
                    }
                }
            }
        }
    }



    public static void main(String[] args) {
        /* Configure command line */
        Options cmdLineOptions = new Options();
        cmdLineOptions.addOption("g", "graph", true, "graph file name");
        cmdLineOptions.addOption("n", "nshards", true, "number of shards");
        cmdLineOptions.addOption("d", "als_dimension", true, "ALS dimensionality (default 20)");

        try {

            /* Parse command line */
            CommandLineParser parser = new PosixParser();
            CommandLine cmdLine =  parser.parse(cmdLineOptions, args);

            /**
             * Pre-process graph if needed
             */
            String baseFilename = cmdLine.getOptionValue("graph");
            int nShards = Integer.parseInt(cmdLine.getOptionValue("nshards"));
            int D = Integer.parseInt((cmdLine.hasOption("als_dimension") ? cmdLine.getOptionValue("als_dimension") : "5"));

            MovieRecommender recommender = new MovieRecommender(baseFilename, nShards, D);
            recommender.execute();

        } catch (Exception err) {
            err.printStackTrace();
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("MovieRecommender", cmdLineOptions);
        }
    }



}
