package edu.cmu.graphchi.walks;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.walks.distributions.RemoteDrunkardCompanion;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Class for running DrunkardMob random walk applications.
 * This can run multiple distinct random walk computations. They are executed
 * simultaneously when iterating over the graph.
 * @author Aapo Kyrola
 */
public class DrunkardMobEngine<VertexDataType, EdgeDataType> {

    private GraphChiEngine<VertexDataType, EdgeDataType> engine;
    private List<DrunkardDriver> drivers;

    private static Logger logger = ChiLogger.getLogger("drunkardmob-engine");
    private DrunkardFactory<VertexDataType, EdgeDataType> factory;

    /**
     * Create the engine
     * @param factory we allow walks to be represented either as ints or as longs (if more
     * information needs to be stored, e.g. to retrieve path information from the walks).  In order
     * to avoid autoboxing, we do a little bit of fancy footwork here.  The caller must pass in an
     * IntDrunkardFactory or a LongDrunkardFactory, and then when processing WalkArrays and
     * DrunkardContexts, they must be cast to IntWalkArrays or LongWalkArrays (and Contexts, and
     * whatever else) in order to get the actual values out.  This way we can keep the primitive
     * typing while still sharing as much code as possible between the int and the long processing.
     */
    public DrunkardMobEngine(String baseFilename, int nShards,
            DrunkardFactory<VertexDataType, EdgeDataType> factory) throws IOException {
        createGraphChiEngine(baseFilename, nShards);
        this.drivers = new ArrayList<DrunkardDriver>();
        this.factory = factory;

        // Disable all edge directions by default
        engine.setDisableInedges(true);
        engine.setDisableOutEdges(true);
        engine.setModifiesInedges(false);
        engine.setModifiesOutedges(false);
    }

    private void createGraphChiEngine(String baseFilename, int nShards) throws IOException {
        this.engine = new GraphChiEngine<VertexDataType, EdgeDataType>(baseFilename, nShards);
        this.engine.setOnlyAdjacency(true);
        this.engine.setVertexDataConverter(null);
        this.engine.setEdataConverter(null);
    }


    public GraphChiEngine<VertexDataType, EdgeDataType> getEngine() {
        return engine;
    }

    public DrunkardJob getJob(String name) {
        for(DrunkardDriver driver : drivers) {
            if (driver.getJob().getName().equals(name)) {
                return driver.getJob();
            }
        }
        return null;
    }

    /**
     * Configure edge data type converter - if you need edge values
     * @param edataConverter
     */
    public void setEdataConverter(BytesToValueConverter<EdgeDataType> edataConverter) {
        engine.setEdataConverter(edataConverter);
        if (edataConverter != null) {
            engine.setOnlyAdjacency(false);
        } else {
            engine.setOnlyAdjacency(true);
        }
    }

    /**
     * Configure vertex data type converter - if you need vertex values
     * @param vertexDataConverter
     */
    public void setVertexDataConverter(BytesToValueConverter<VertexDataType> vertexDataConverter) {
        engine.setVertexDataConverter(vertexDataConverter);
    }

    /**
     * Adds a random walk job.  Use run() to run all the jobs.
     * @param edgeDirection which direction edges need to be considered
     * @param callback your walk logic
     * @param companion object that keeps track of the walks
     * @return the job object
     */
    public DrunkardJob addJob(String jobName, EdgeDirection edgeDirection,
                              WalkUpdateFunction<VertexDataType, EdgeDataType> callback,
                              RemoteDrunkardCompanion companion) {

        /* Configure engine parameters */
        switch(edgeDirection) {
            case IN_AND_OUT_EDGES:
                engine.setDisableInedges(false);
                engine.setDisableOutEdges(false);
                break;
            case IN_EDGES:
                engine.setDisableInedges(false);
                break;
            case OUT_EDGES:
                engine.setDisableOutEdges(false);
                break;
        }

        /**
         * Create job object and the driver-object.
         */
        DrunkardJob job = new DrunkardJob(jobName, companion, engine.numVertices(), factory);
        drivers.add(factory.createDrunkardDriver(job, callback));
        return job;
    }


    public void run(int numIterations) throws IOException, RemoteException {
        engine.setEnableScheduler(true);



        int memoryBudget = 1200;
        if (System.getProperty("membudget") != null) memoryBudget = Integer.parseInt(System.getProperty("membudget"));

        engine.setMemoryBudgetMb(memoryBudget);
        engine.setEnableDeterministicExecution(false);
        engine.setAutoLoadNext(false);
        engine.setVertexDataConverter(null);
        engine.setMaxWindow(10000000); // Handle maximum 10M vertices a time.

        long t = System.currentTimeMillis();
        for(DrunkardDriver driver : drivers) {
            if (driver.getJob().getWalkManager() == null) {
                throw new IllegalStateException("You need to configure walks by calling DrunkardJob.configureXXX()");
            }
            driver.initWalks();
        }
        long initTime = System.currentTimeMillis() - t;

        /* Run GraphChi */
        logger.info("Starting running drunkard jobs (" + drivers.size() + " jobs)");
        engine.run(new GraphChiDrunkardWrapper(), numIterations);


        /* Finish up */
        for(DrunkardDriver driver: drivers) {
            driver.spinUntilFinish();
        }
    }

    public VertexIdTranslate getVertexIdTranslate() {
        return engine.getVertexIdTranslate();
    }


    /**
     * Multiplex for DrunkardDrivers.
     */
    protected class GraphChiDrunkardWrapper implements GraphChiProgram<VertexDataType, EdgeDataType> {

        long iterationStart;

        public GraphChiDrunkardWrapper() {
        }

        @Override
        public void update(ChiVertex<VertexDataType, EdgeDataType> vertex, GraphChiContext context) {
            try {
            /* Buffer management. TODO: think, this is too complex after adding the multiplex */
                if (context.getThreadLocal() == null) {
                    ArrayList<LocalWalkBuffer> multiplexedLocalBuffers = new ArrayList<LocalWalkBuffer>(drivers.size());
                    for(DrunkardDriver driver: drivers) {
                        LocalWalkBuffer buf = factory.createLocalWalkBuffer();
                        driver.addLocalBuffer(buf);
                        multiplexedLocalBuffers.add(buf);
                    }
                    context.setThreadLocal(multiplexedLocalBuffers);
                }

                final ArrayList<LocalWalkBuffer> multiplexedLocalBuffers = (ArrayList<LocalWalkBuffer>) context.getThreadLocal();

                int i = 0;
                for(DrunkardDriver driver : drivers) {
                    driver.update(vertex, context, multiplexedLocalBuffers.get(i++));
                }
            } catch (Exception err) {
                err.printStackTrace();
            }
        }

        @Override
        public void beginIteration(GraphChiContext ctx) {
            iterationStart = System.currentTimeMillis();
            for(DrunkardDriver driver : drivers) {
                driver.beginIteration(ctx);
            }
        }

        @Override
        public void endIteration(GraphChiContext ctx) {
            for(DrunkardDriver driver : drivers) {
                driver.endIteration(ctx);
            }

            long iterationTime = System.currentTimeMillis() - iterationStart;
        }

        @Override
        public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
            for(DrunkardDriver driver : drivers) {
                driver.beginInterval(ctx, interval);
            }
        }

        @Override
        public void endInterval(GraphChiContext ctx, VertexInterval interval) {
            for(DrunkardDriver driver : drivers) {
                driver.endInterval(ctx, interval);
            }
        }

        @Override
        public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
            for(DrunkardDriver driver : drivers) {
                driver.beginSubInterval(ctx, interval);
            }
        }

        @Override
        public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
            for(DrunkardDriver driver : drivers) {
                driver.endSubInterval(ctx, interval);
            }
        }
    }
}
