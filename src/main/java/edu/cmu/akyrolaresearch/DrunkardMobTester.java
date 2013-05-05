package edu.cmu.akyrolaresearch;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.preprocessing.VertexProcessor;
import edu.cmu.graphchi.util.IdCount;
import edu.cmu.graphchi.walks.DrunkardContext;
import edu.cmu.graphchi.walks.DrunkardJob;
import edu.cmu.graphchi.walks.DrunkardMobEngine;
import edu.cmu.graphchi.walks.WalkUpdateFunction;
import edu.cmu.graphchi.walks.WeightedHopper;
import edu.cmu.graphchi.walks.distributions.DrunkardCompanion;
import edu.cmu.graphchi.walks.distributions.RemoteDrunkardCompanion;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Computes estimate of personalized pagerank using the DrunkardMobEngine.
 * <b>Note:</b> this version omits walks to adjacent vertices, and thus could be a
 * basis for recommendation engine. To remove that functionality, modify method
 * getNotTrackedVertices()
 * @author Aapo Kyrola
 */
public class DrunkardMobTester implements WalkUpdateFunction<EmptyType, EmptyType> {

    private static double RESET_PROBABILITY = 0.15;
    private static Logger logger = ChiLogger.getLogger("personalized-pagerank");
    private DrunkardMobEngine<EmptyType, EmptyType>  drunkardMobEngine;
    private String baseFilename;
    private int firstSource;
    private int numSources;
    private int numWalksPerSource;
    private String companionUrl;

    private ExperimentTiming expTiming;

    public DrunkardMobTester(String companionUrl, String baseFilename, int nShards, int firstSource, int numSources, int walksPerSource) throws Exception{
        this.baseFilename = baseFilename;
        this.drunkardMobEngine = new DrunkardMobEngine<EmptyType, EmptyType>(baseFilename, nShards);

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
       RemoteDrunkardCompanion companion;
        if (companionUrl.equals("local")) {
            companion = new DrunkardCompanion(4, Runtime.getRuntime().maxMemory() / 3);
        } else if (companionUrl.equals("none")) {
            companion = new RemoteDrunkardCompanion() {
                @Override
                public void setAvoidList(int sourceIdx, int[] avoidList) throws RemoteException {

                }

                @Override
                public void setSources(int[] sources) throws RemoteException {

                }

                @Override
                public void processWalks(int[] walks, int[] atVertices) throws RemoteException {

                }

                @Override
                public void outputDistributions(String outputFile) throws RemoteException {

                }

                @Override
                public void outputDistributions(String outputFile, int nTop) throws RemoteException {

                }

                @Override
                public IdCount[] getTop(int vertexId, int nTop) throws RemoteException {
                    return new IdCount[0];
                }
            };
        }  else {
            companion = (RemoteDrunkardCompanion) Naming.lookup(companionUrl);
        }

        expTiming = new ExperimentTiming();
        expTiming.setCompanionAddress(companionUrl);
        expTiming.setFirstSource(firstSource);
        expTiming.setNumSources(numSources);
        expTiming.setWalksPerSource(numWalksPerSource);
        expTiming.setGraphName(new File(baseFilename).getName());


        /* Configure walk sources. Note, GraphChi's internal ids are used. */
        long t = System.currentTimeMillis();
        DrunkardJob drunkardJob = this.drunkardMobEngine.addJob("personalizedPageRank",
                EdgeDirection.OUT_EDGES, this, companion);


        drunkardJob.configureSourceRangeInternalIds(firstSource, numSources, numWalksPerSource);


        drunkardMobEngine.run(numIters, expTiming);


        /* If local, shutdown the companion */
        if (companion instanceof DrunkardCompanion) {
            ((DrunkardCompanion) companion).close();
        }

        expTiming.setRunTimeTotal((System.currentTimeMillis() - t) * 0.001);

        ExperimentTiming.insert(expTiming);

        System.exit(0);
    }

    /**
     * WalkUpdateFunction interface implementations
     */
    @Override
    public void processWalksAtVertex(int[] walks,
                                     ChiVertex<EmptyType, EmptyType> vertex,
                                     DrunkardContext drunkardContext,
                                     Random randomGenerator) {
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
     * Instruct drunkardMob not to track visits to this vertex's immediate out-neighbors.
     */
    public int[] getNotTrackedVertices(ChiVertex<EmptyType, EmptyType> vertex) {
        int[] notCounted = new int[1 + vertex.numOutEdges()];
        for(int i=0; i < vertex.numOutEdges(); i++) {
            notCounted[i + 1] = vertex.getOutEdgeId(i);
        }
        notCounted[0] = vertex.getId();
        return notCounted;
    }

    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<EmptyType, EmptyType>(graphName, numShards, null, null, null, null);
    }

    public static void main(String[] args) throws Exception {

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
             * Preprocess graph if needed
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

            DrunkardMobTester pp = new DrunkardMobTester(companionUrl, baseFilename, nShards,
                    firstSource, numSources, walksPerSource);
            pp.execute(nIters);

        } catch (Exception err) {
            err.printStackTrace();
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("PersonalizedPageRank", cmdLineOptions);
        }
    }
}
