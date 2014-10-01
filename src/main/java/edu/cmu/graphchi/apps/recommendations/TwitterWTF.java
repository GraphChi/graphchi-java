package edu.cmu.graphchi.apps.recommendations;


import edu.cmu.graphchi.*;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

import edu.cmu.graphchi.preprocessing.VertexProcessor;
import edu.cmu.graphchi.queries.VertexQuery;
import edu.cmu.graphchi.util.IdCount;
import edu.cmu.graphchi.walks.DrunkardContext;
import edu.cmu.graphchi.walks.DrunkardJob;
import edu.cmu.graphchi.walks.DrunkardMobEngine;
import edu.cmu.graphchi.walks.IntDrunkardContext;
import edu.cmu.graphchi.walks.IntDrunkardFactory;
import edu.cmu.graphchi.walks.IntWalkArray;
import edu.cmu.graphchi.walks.WalkArray;
import edu.cmu.graphchi.walks.WalkUpdateFunction;
import edu.cmu.graphchi.walks.WeightedHopper;
import edu.cmu.graphchi.walks.distributions.IntDrunkardCompanion;
import edu.cmu.graphchi.walks.distributions.RemoteDrunkardCompanion;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *
 * Emulates complete Twitter's Who-To-Follow (WTF) algorithm's SALSA part as described in WWW'13 paper
 * WTF: The Who to Follow Service at Twitter: http://www.stanford.edu/~rezab/papers/wtf_overview.pdf
 *
 * Step 1: Compute a "circle of trust" (i.e top 500 visits of an egocentric random walk) for N users
 * Step 2: For each user i, compute SALSA where hubs=circle of trust, authorities=their top followers
 * Step 3: Recommend top K of the authorities of the SALSA.
 *
 * DrunkardMob algorithm is used for the random walks and for efficient loading of followers
 * for the circle of trust, graph-query capabilities on edu.cmu.graphchiquerues.VertexQuery are used.
 *
 * Example parameters:
 * <pre>
 *   --graph=/Users/akyrola/graphs/twitter_rv.net --nshards=24 --niters=5 --nsources=20000 --firstsource=0 --walkspersource=3000
 * </pre>
 *
 * Remember to allcoate enough memory, for example -Xmx6G
 *
 * @author Aapo Kyrola
 */
public class TwitterWTF implements WalkUpdateFunction<EmptyType, EmptyType> {

    private static double RESET_PROBABILITY = 0.15;
    private static Logger logger = ChiLogger.getLogger("twitter-wtf");
    private DrunkardMobEngine<EmptyType, EmptyType>  drunkardMobEngine;
    private String baseFilename;
    private int firstSource;
    private int numSources;
    private int numShards;
    private int numWalksPerSource;
    private int salsaCacheSize = Integer.parseInt(System.getProperty("salsacache", "100000"));
    private String companionUrl;

    public TwitterWTF(String companionUrl, String baseFilename, int nShards, int firstSource, int numSources, int walksPerSource) throws Exception{
        this.baseFilename = baseFilename;
        this.drunkardMobEngine = new DrunkardMobEngine<EmptyType, EmptyType>(baseFilename, nShards,
                new IntDrunkardFactory());

        this.numShards = nShards;
        this.companionUrl = companionUrl;
        this.firstSource = firstSource;
        this.numSources = numSources;
        this.numWalksPerSource = walksPerSource;
    }

    private void execute(int numIters) throws Exception {
        File graphFile = new File(baseFilename);

        /** Use local drunkard mob companion. You can also pass a remote reference
         *  by using Naming.lookup("rmi://my-companion")
         */
        final RemoteDrunkardCompanion companion;
        if (companionUrl.equals("local")) {
            companion = new IntDrunkardCompanion(4, Runtime.getRuntime().maxMemory() / 3);
        }  else {
            companion = (RemoteDrunkardCompanion) Naming.lookup(companionUrl);
        }

        /* Step 1: Compute random walks */
        /* Configure walk sources. Note, GraphChi's internal ids are used. */

        DrunkardJob drunkardJob = this.drunkardMobEngine.addJob("twitterwtf",
                EdgeDirection.OUT_EDGES, this, companion);

        drunkardJob.configureSourceRangeInternalIds(firstSource, numSources, numWalksPerSource);
        drunkardMobEngine.run(numIters);


        // Empty from memory so can use cache in the SALSA
        this.drunkardMobEngine = null;
        drunkardJob = null;

        /* Step 2: SALSA */
        final int circleOfTrustSize = 200;

        final long startTime = System.currentTimeMillis();

        final AtomicInteger numRecs = new AtomicInteger();
        final AtomicInteger pending = new AtomicInteger();

        // FIXME: hardcoded
        ExecutorService executor = Executors.newFixedThreadPool(4);

        logger.info("Started 4 threads");


        // Each thread need to have a local query service so the file descriptors don't clash.
        final ThreadLocal<VertexQuery> queryService = new ThreadLocal<VertexQuery>() {
            @Override
            protected VertexQuery initialValue() {
                try {
                    return new VertexQuery(baseFilename, numShards);
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    throw new RuntimeException(ioe);
                }
            }


        };

        //
 
        long t = System.currentTimeMillis();

        for(int vertexId=firstSource; vertexId < firstSource+numSources; vertexId++) {
            final int _vertexId = vertexId;
            pending.incrementAndGet();
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        CircleOfTrustSalsa csalsa = new CircleOfTrustSalsa(queryService.get(), salsaCacheSize);
                        computeRecs(companion, circleOfTrustSize, startTime, csalsa, numRecs, _vertexId);

                    } catch (Exception err) {
                        err.printStackTrace();
                    }
                    pending.decrementAndGet();
                }
            });
        }

        while(pending.get() > 0) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
            System.out.println("Pending WTF queries: " + pending.get());
        }
 
        System.out.println("WTF-recs," + (System.currentTimeMillis() - t));

    }

    private void computeRecs(RemoteDrunkardCompanion companion, int circleOfTrustSize, long startTime, CircleOfTrustSalsa csalsa, AtomicInteger numRecs, int vertexId) throws IOException {
        /* Get circle of trust from the DrunkardCompanion */
        IdCount[] topVisits = companion.getTop(vertexId, circleOfTrustSize);

        HashSet<Integer> circleOfTrust = new HashSet<Integer>(topVisits.length);
        for(IdCount idc: topVisits) {
            circleOfTrust.add(idc.id);
        }

        /* Initialize and run SALSA */
        csalsa.initializeGraph(circleOfTrust);
        csalsa.computeSALSA(4);

        // Make a list of immediate neighbors, which should not be recommended
        // NOTE: the companion would have that list also!
        HashSet<Integer> doNotRecommend = csalsa.getQueryService().queryOutNeighbors(vertexId);
        doNotRecommend.add(vertexId);

        /* Get SALSA's top results and print */

        ArrayList<CircleOfTrustSalsa.SalsaVertex> recommendations = csalsa.topAuthorities(10, doNotRecommend);


        /*logger.info("Recommendations for " + csalsa.namify(vertexIdTranslate.backward(vertexId)) + " (" + vertexIdTranslate.backward(vertexId) + ")");
   for(CircleOfTrustSalsa.SalsaVertex sv : recommendations) {
       int originalId = vertexIdTranslate.backward(sv.id);
       logger.info("  recommend: " + " = " + originalId + " " + csalsa.namify(originalId) + " (" + sv.value + ")");
   } */

        int numRecsNow = numRecs.incrementAndGet();
        if (numRecsNow % 100 == 0) {
            long t = System.currentTimeMillis() - startTime;
            logger.info("Computed recommendations for " + numRecsNow + " users in " + t + "ms");
            logger.info("Average: " + (double)t / (vertexId - firstSource + 1) + "ms");
        }
    }

    /**
     * WalkUpdateFunction interface implementations
     */
    @Override
    public void processWalksAtVertex(WalkArray walkArray,
                                     ChiVertex<EmptyType, EmptyType> vertex,
                                     DrunkardContext drunkardContext_,
                                     Random randomGenerator) {
        int[] walks = ((IntWalkArray)walkArray).getArray();
        IntDrunkardContext drunkardContext = (IntDrunkardContext) drunkardContext_;
        int numWalks = walks.length;
        int numOutEdges = vertex.numOutEdges();

        // Advance each walk to a random out-edge (if any)
        if (numOutEdges > 0) {
            for(int i=0; i < numWalks; i++) {
                int walk = walks[i];

                // Reset?
                if (randomGenerator.nextDouble() < RESET_PROBABILITY) {
                    drunkardContext.resetWalk(walk, false);
                } else {
                    int nextHop  = vertex.getOutEdgeId(randomGenerator.nextInt(numOutEdges));

                    // Optimization to tell the manager that walks that have just been started
                    // need not to be tracked.
                    boolean shouldTrack = !drunkardContext.isWalkStartedFromVertex(walk);
                    drunkardContext.forwardWalkTo(walk, nextHop, shouldTrack);
                }
            }

        } else {
            // Reset all walks -- no where to go from here
            for(int i=0; i < numWalks; i++) {
                drunkardContext.resetWalk(walks[i], false);
            }
        }
    }

    @Override
    /**
     * Only ignore the current vertex
     */
    public int[] getNotTrackedVertices(ChiVertex<EmptyType, EmptyType> vertex) {
        int[] notCounted = new int[1];
        notCounted[0] = vertex.getId();
        return notCounted;
    }


    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<EmptyType, EmptyType>(graphName, numShards, null, null, null, null);
    }

    public static void main(String[] args) throws Exception {

        long t = System.currentTimeMillis();

        /* Configure command line */
        Options cmdLineOptions = new Options();
        cmdLineOptions.addOption("g", "graph", true, "graph file name");
        cmdLineOptions.addOption("n", "nshards", true, "number of shards");
        cmdLineOptions.addOption("t", "filetype", true, "filetype (edgelist|adjlist)");
        cmdLineOptions.addOption("f", "firstsource", true, "id of the first source vertex (internal id)");
        cmdLineOptions.addOption("s", "nsources", true, "number of sources");
        cmdLineOptions.addOption("w", "walkspersource", true, "number of walks to start from each source");
        cmdLineOptions.addOption("i", "niters", true, "number of iterations");
        cmdLineOptions.addOption("u", "companion", true, "RMI url to the DrunkardCompanion or 'local' (default)");

        try {

            /* Parse command line */
            CommandLineParser parser = new PosixParser();
            CommandLine cmdLine =  parser.parse(cmdLineOptions, args);

            /**
             * Pre-process graph if needed
             */
            String baseFilename = cmdLine.getOptionValue("graph");
            int nShards = Integer.parseInt(cmdLine.getOptionValue("nshards"));
            String fileType = (cmdLine.hasOption("filetype") ? cmdLine.getOptionValue("filetype") : null);

            /* Create shards */
            if (baseFilename.equals("pipein")) {     // Allow piping graph in
                FastSharder sharder = createSharder(baseFilename, nShards);
                sharder.shard(System.in, fileType);
            } else {
                FastSharder sharder = createSharder(baseFilename, nShards);
                if (!new File(ChiFilenames.getFilenameIntervals(baseFilename, nShards)).exists()) {
                    sharder.shard(new FileInputStream(new File(baseFilename)), fileType);
                } else {
                    logger.info("Found shards -- no need to pre-process");
                }
            }

            // Run
            int firstSource = Integer.parseInt(cmdLine.getOptionValue("firstsource"));
            int numSources = Integer.parseInt(cmdLine.getOptionValue("nsources"));
            int walksPerSource = Integer.parseInt(cmdLine.getOptionValue("walkspersource"));
            int nIters = Integer.parseInt(cmdLine.getOptionValue("niters"));
            String companionUrl = cmdLine.hasOption("companion") ? cmdLine.getOptionValue("companion") : "local";

            TwitterWTF pp = new TwitterWTF(companionUrl, baseFilename, nShards,
                    firstSource, numSources, walksPerSource);
            pp.execute(nIters);


            System.out.println("WTF-log," + (System.currentTimeMillis() - t) + "," + firstSource +"," + (firstSource + numSources - 1) +
                    "," + walksPerSource + "," + nIters);

            System.exit(0);
        } catch (Exception err) {
            err.printStackTrace();
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TwitterWTF", cmdLineOptions);
        }

    }
}
