package edu.cmu.graphchi.engine;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.engine.auxdata.DegreeData;
import edu.cmu.graphchi.engine.auxdata.VertexData;
import edu.cmu.graphchi.engine.auxdata.VertexDegree;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.shards.MemoryShard;
import edu.cmu.graphchi.shards.SlidingShard;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class GraphChiEngine <VertexDataType, EdgeDataType> {

    protected String baseFilename;
    protected int nShards;
    protected ArrayList<VertexInterval> intervals;
    protected ArrayList<SlidingShard<EdgeDataType>> slidingShards;

    protected BytesToValueConverter<EdgeDataType> edataConverter;
    protected BytesToValueConverter<VertexDataType> vertexDataConverter;

    protected GraphChiContext chiContext = new GraphChiContext();
    private DataBlockManager blockManager;
    private ExecutorService parallelExecutor;
    private ExecutorService loadingExecutor;
    private DegreeData degreeHandler;
    private VertexData<VertexDataType> vertexDataHandler;

    protected int subIntervalStart, subIntervalEnd;

    protected int maxWindow = 20000000;
    protected boolean enableScheduler = false;
    protected boolean onlyAdjacency = false;
    protected BitsetScheduler scheduler = null;
    protected long nupdates = 0;
    protected boolean enableDeterministicExecution = true;
    private boolean useStaticWindowSize = false;
    protected long memBudget;
    protected VertexIdTranslate vertexIdTranslate;


    private static final Logger logger = LoggingInitializer.getLogger("engine");

    /* Automatic loading of next window */
    private boolean autoLoadNext = false; // Only for only-adjacency cases!
    private boolean skipZeroDegreeVertices = false;

    private FutureTask<IntervalData> nextWindow;

    /* Metrics */
    private final Timer loadTimer = Metrics.defaultRegistry().newTimer(GraphChiEngine.class, "shard-loading", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer executionTimer = Metrics.defaultRegistry().newTimer(GraphChiEngine.class, "execute-updates", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer waitForFutureTimer = Metrics.defaultRegistry().newTimer(GraphChiEngine.class, "wait-for-future", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer initVerticesTimer = Metrics.defaultRegistry().newTimer(GraphChiEngine.class, "init-vertices", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer determineNextWindowTimer = Metrics.defaultRegistry().newTimer(GraphChiEngine.class, "det-next-window", TimeUnit.SECONDS, TimeUnit.MINUTES);


    protected boolean modifiesInedges = true, modifiesOutedges = true;
    private boolean disableInEdges = false, disableOutEdges = false;

    public GraphChiEngine(String baseFilename, int nShards) throws FileNotFoundException, IOException {
        this.baseFilename = baseFilename;
        this.nShards = nShards;
        loadIntervals();
        blockManager = new DataBlockManager();
        degreeHandler = new DegreeData(baseFilename);

        File vertexIdTranslateFile = new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, nShards));
        if (vertexIdTranslateFile.exists()) {
            vertexIdTranslate = VertexIdTranslate.fromFile(vertexIdTranslateFile);
        } else {
            vertexIdTranslate = VertexIdTranslate.identity();
        }
        chiContext.setVertexIdTranslate(vertexIdTranslate);

        memBudget = Runtime.getRuntime().maxMemory() / 4;
        if (Runtime.getRuntime().maxMemory() < 256 * 1024 * 1024)
            throw new IllegalArgumentException("Java Virtual Machine has only " + memBudget + "bytes maximum memory." +
                    " Please run the JVM with at least 256 megabytes of memory using -Xmx256m. For better performance, use higher value");

    }

    public ArrayList<VertexInterval> getIntervals() {
        return intervals;
    }

    protected void loadIntervals() throws FileNotFoundException, IOException {
        intervals = ChiFilenames.loadIntervals(baseFilename, nShards);
    }


    /**
     * Set the memorybudget in megabytes. Default is JVM's max memory / 4
     * @param mb
     */
    public void setMemoryBudgetMb(long mb) {
        memBudget = mb * 1024 * 1024;
    }

    public long getMemoryBudget() {
        return memBudget;
    }


    public void setSkipZeroDegreeVertices(boolean skipZeroDegreeVertices) {
        this.skipZeroDegreeVertices = skipZeroDegreeVertices;
    }

    public int numVertices() {
        return 1 + intervals.get(intervals.size() - 1).getLastVertex();
    }

    protected void initializeSlidingShards() throws IOException {
        slidingShards = new ArrayList<SlidingShard<EdgeDataType> >();
        for(int p=0; p < nShards; p++) {
            String edataFilename = (onlyAdjacency ? null : ChiFilenames.getFilenameShardEdata(baseFilename, edataConverter, p, nShards));
            String adjFilename = ChiFilenames.getFilenameShardsAdj(baseFilename, p, nShards);

            SlidingShard<EdgeDataType> slidingShard = new SlidingShard<EdgeDataType>(edataFilename, adjFilename, intervals.get(p).getFirstVertex(),
                    intervals.get(p).getLastVertex());
            slidingShard.setConverter(edataConverter);
            slidingShard.setDataBlockManager(blockManager);
            slidingShard.setModifiesOutedges(modifiesOutedges);
            slidingShard.setOnlyAdjacency(onlyAdjacency);
            slidingShards.add(slidingShard);

        }
    }

    protected MemoryShard<EdgeDataType> createMemoryShard(int intervalStart, int intervalEnd, int execInterval) {
        String edataFilename = (onlyAdjacency ? null : ChiFilenames.getFilenameShardEdata(baseFilename, edataConverter, execInterval, nShards));
        String adjFilename = ChiFilenames.getFilenameShardsAdj(baseFilename, execInterval, nShards);

        MemoryShard<EdgeDataType> newMemoryShard = new MemoryShard<EdgeDataType>(edataFilename, adjFilename, intervals.get(execInterval).getFirstVertex(),
                intervals.get(execInterval).getLastVertex());
        newMemoryShard.setConverter(edataConverter);
        newMemoryShard.setDataBlockManager(blockManager);
        newMemoryShard.setOnlyAdjacency(onlyAdjacency);
        return newMemoryShard;
    }


    public void run(GraphChiProgram<VertexDataType, EdgeDataType> program, int niters) throws IOException {

        int nprocs = 4;
        if (Runtime.getRuntime().availableProcessors() > nprocs) {
            nprocs = Runtime.getRuntime().availableProcessors();
        }

        if (System.getProperty("num_threads") != null)
            nprocs = Integer.parseInt(System.getProperty("num_threads"));

        logger.info(":::::::: Using " + nprocs + " execution threads :::::::::");

        parallelExecutor = Executors.newFixedThreadPool(nprocs);
        loadingExecutor = Executors.newFixedThreadPool(4);

        chiContext.setNumIterations(niters);

        long startTime = System.currentTimeMillis();
        initializeSlidingShards();

        if (enableScheduler) {
            initializeScheduler();
            chiContext.setScheduler(scheduler);
            scheduler.addAllTasks();
            logger.info("Using scheduler!");
        }  else {
            chiContext.setScheduler(new MockScheduler());
        }


        if (disableInEdges) {
            ChiVertex.disableInedges = true;
        }
        if (disableOutEdges) {
            ChiVertex.disableOutedges = true;
        }

        /* Initialize vertex-data handler */
        if (vertexDataConverter != null) {
            vertexDataHandler = new VertexData<VertexDataType>(numVertices(), baseFilename, vertexDataConverter, skipZeroDegreeVertices);
            vertexDataHandler.setBlockManager(blockManager);
        }

        for(int iter=0; iter < niters; iter++) {
            /* Wait for executor have finished all writes */
            while (!blockManager.empty()) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {}
            }
            blockManager.reset();
            chiContext.setIteration(iter);
            chiContext.setNumVertices(numVertices());
            program.beginIteration(chiContext);

            if (scheduler != null) {
                if (iter > 0 && !scheduler.hasTasks()) {
                    logger.info("No new tasks to run. Terminating.");
                    break;
                }
                scheduler.reset();
            }

            for(int execInterval=0; execInterval < nShards; ++execInterval) {
                int intervalSt = intervals.get(execInterval).getFirstVertex();
                int intervalEn = intervals.get(execInterval).getLastVertex();

                logger.info((System.currentTimeMillis() - startTime) * 0.001 + "s: iteration: " + iter + ", interval: " + intervalSt + " -- " + intervalEn);

                program.beginInterval(chiContext, intervals.get(execInterval));

                MemoryShard<EdgeDataType> memoryShard = null;
                if (!disableInEdges) {
                    if (!onlyAdjacency || !autoLoadNext || nextWindow == null) {
                        if (!disableOutEdges) slidingShards.get(execInterval).flush();     // MESSY!
                        memoryShard = createMemoryShard(intervalSt, intervalEn, execInterval);
                    } else {
                        memoryShard = null;
                    }
                }

                subIntervalStart = intervalSt;

                while (subIntervalStart <= intervalEn) {
                    int adjMaxWindow = maxWindow;
                    if (Integer.MAX_VALUE - subIntervalStart < maxWindow) adjMaxWindow = Integer.MAX_VALUE - subIntervalStart - 1;

                    if (anyVertexScheduled(subIntervalStart, Math.min(intervalEn, subIntervalStart + adjMaxWindow ))) {
                        ChiVertex<VertexDataType, EdgeDataType>[] vertices = null;
                        int vertexBlockId = -1;

                        if (!autoLoadNext || nextWindow == null) {
                            try {
                                subIntervalEnd = determineNextWindow(subIntervalStart, Math.min(intervalEn, subIntervalStart + adjMaxWindow ));
                            } catch (NoEdgesInIntervalException nie) {
                                logger.info("No edges, skip: " + subIntervalStart + " -- " + subIntervalEnd);
                                subIntervalEnd = subIntervalStart + adjMaxWindow;
                                subIntervalStart = subIntervalEnd + 1;
                                continue;
                            }
                            int nvertices = subIntervalEnd - subIntervalStart + 1;

                            logger.info("Subinterval:: " + subIntervalStart + " -- " + subIntervalEnd + " (iteration " + iter + ")");

                            vertices = new ChiVertex[nvertices];

                            logger.info("Init vertices...");
                            vertexBlockId = initVertices(nvertices, subIntervalStart, vertices);

                            logger.info("Loading...");
                            long t0 = System.currentTimeMillis();
                            loadBeforeUpdates(execInterval, vertices, memoryShard, subIntervalStart, subIntervalEnd);
                            logger.info("Load took: " + (System.currentTimeMillis() - t0) + "ms");
                        } else {
                            /* This is a mess! */
                            try {
                                long tf = System.currentTimeMillis();
                                final TimerContext _timer = waitForFutureTimer.time();
                                IntervalData next = nextWindow.get();

                                memoryShard = next.getMemShard();
                                _timer.stop();
                                logger.info("Waiting for future task loading took " + (System.currentTimeMillis() - tf) + " ms");
                                if (subIntervalStart != next.getSubInterval().getFirstVertex())
                                    throw new IllegalStateException("Future loaders interval does not match the expected one! " +
                                            subIntervalStart + " != " + next.getSubInterval().getFirstVertex());
                                subIntervalEnd = next.getSubInterval().getLastVertex();
                                vertexBlockId = next.getVertexBlockId();
                                vertices = next.getVertices();
                                nextWindow = null;
                            } catch (Exception err) {
                                throw new RuntimeException(err);
                            }
                        }

                        if (autoLoadNext) {
                            /* Start a future for loading the next window */
                            adjMaxWindow = maxWindow;
                            if (Integer.MAX_VALUE - subIntervalEnd < maxWindow) adjMaxWindow = Integer.MAX_VALUE - subIntervalEnd - 1;

                            if (subIntervalEnd + 1 <= intervalEn) {
                                nextWindow = new FutureTask<IntervalData>(new AutoLoaderTask(new VertexInterval(subIntervalEnd + 1,
                                        Math.min(intervalEn, subIntervalEnd + 1 + adjMaxWindow)), execInterval, memoryShard));
                            } else if (execInterval < nShards - 1) {
                                int nextIntervalSt = intervals.get(execInterval + 1).getFirstVertex();
                                int nextIntervalEn = intervals.get(execInterval + 1).getLastVertex();

                                slidingShards.get(execInterval).setOffset(memoryShard.getStreamingOffset(),
                                        memoryShard.getStreamingOffsetVid(), memoryShard.getStreamingOffsetEdgePtr());
                                nextWindow = new FutureTask<IntervalData>(new AutoLoaderTask(new VertexInterval(nextIntervalSt,
                                        Math.min(nextIntervalEn, nextIntervalSt + 1 + adjMaxWindow)), execInterval + 1,
                                        createMemoryShard(nextIntervalSt, nextIntervalEn, execInterval + 1)));

                            }
                            if (nextWindow != null)
                                loadingExecutor.submit(nextWindow);

                        }
                        /* Clear scheduler bits */
                        if (scheduler != null) scheduler.removeTasks(subIntervalStart, subIntervalEnd);

                        chiContext.setCurInterval(new VertexInterval(subIntervalStart, subIntervalEnd));
                        program.beginSubInterval(chiContext, new VertexInterval(subIntervalStart, subIntervalEnd));

                        long t1 = System.currentTimeMillis();
                        execUpdates(program, vertices);
                        logger.info("Update exec: " + (System.currentTimeMillis() - t1) + " ms.");

                        // Write vertices (async)
                        final int _firstVertex = subIntervalStart;
                        final int _blockId = vertexBlockId;
                        parallelExecutor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    vertexDataHandler.releaseAndCommit(_firstVertex, _blockId);
                                } catch (IOException ioe) {
                                    ioe.printStackTrace();
                                }
                            }
                        });

                        subIntervalStart = subIntervalEnd + 1;

                        program.endSubInterval(chiContext, new VertexInterval(subIntervalStart, subIntervalEnd));

                    }  else {
                        subIntervalEnd = subIntervalStart + adjMaxWindow;
                        logger.info("Skipped interval - no vertices scheduled. " + subIntervalStart + " -- " + subIntervalEnd);

                        subIntervalStart = subIntervalEnd + 1;
                    }
                }


                /* Commit */
                if (!disableInEdges) {
                    memoryShard.commitAndRelease(modifiesInedges, modifiesOutedges);
                    if (!disableOutEdges && !autoLoadNext) {
                        slidingShards.get(execInterval).setOffset(memoryShard.getStreamingOffset(),
                                memoryShard.getStreamingOffsetVid(), memoryShard.getStreamingOffsetEdgePtr());
                    }
                }
            }

            for(SlidingShard shard : slidingShards) {
                shard.flush();
                shard.setOffset(0, 0, 0);
            }
            program.endIteration(chiContext);
        }    // Iterations

        parallelExecutor.shutdown();
        loadingExecutor.shutdown();
        logger.info("Engine finished in: " + (System.currentTimeMillis() - startTime) * 0.001 + " secs.");
        logger.info("Updates: " + nupdates);
    }

    private boolean anyVertexScheduled(int subIntervalStart, int lastVertex) {
        if (!enableScheduler) return true;

        for(int i=subIntervalStart; i<= lastVertex; i++) {
            if (scheduler.isScheduled(i)) return true;
        }
        return false;
    }

    private void initializeScheduler() {
        scheduler = new BitsetScheduler(numVertices());
    }

    private void execUpdates(final GraphChiProgram<VertexDataType, EdgeDataType> program,
                             final ChiVertex<VertexDataType, EdgeDataType>[] vertices) {
        if (vertices == null || vertices.length == 0) return;
        TimerContext _timer = executionTimer.time();
        if (Runtime.getRuntime().availableProcessors() == 1) {
            /* Sequential updates */
            for(ChiVertex<VertexDataType, EdgeDataType> vertex : vertices) {
                if (vertex != null) {
                    nupdates++;
                    program.update(vertex, chiContext);
                }
            }
        } else {
            final Object termlock = new Object();
            final int chunkSize = 1 + vertices.length / 64;

            final int nWorkers = vertices.length / chunkSize + 1;
            final AtomicInteger countDown = new AtomicInteger(1 + nWorkers);

            if (!enableDeterministicExecution) {
                for(ChiVertex<VertexDataType, EdgeDataType> vertex : vertices) {
                    if (vertex != null) vertex.parallelSafe = true;
                }
            }

            /* Parallel updates. One thread for non-parallel safe updates, others
     updated in parallel. This guarantees deterministic execution. */

            /* Non-safe updates */
            parallelExecutor.submit(new Runnable() {
                public void run() {
                    int thrupdates = 0;
                    GraphChiContext threadContext = chiContext.clone(0);
                    try {
                        for(ChiVertex<VertexDataType, EdgeDataType> vertex : vertices) {
                            if (vertex != null && !vertex.parallelSafe) {
                                thrupdates++;
                                program.update(vertex, threadContext);
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }  finally {
                        int pending = countDown.decrementAndGet();
                        synchronized (termlock) {
                            nupdates += thrupdates;
                            if (pending == 0) {
                                termlock.notifyAll();;
                            }
                        }
                    }
                }
            });


            /* Parallel updates */
            for(int thrId = 0; thrId < nWorkers; thrId++) {
                final int myId = thrId;
                final int chunkStart = myId * chunkSize;
                final int chunkEnd = chunkStart + chunkSize;

                parallelExecutor.submit(new Runnable() {

                    public void run() {
                        int thrupdates = 0;
                        GraphChiContext threadContext = chiContext.clone(1 + myId);

                        try {
                            int end = chunkEnd;
                            if (end > vertices.length) end = vertices.length;
                            for(int i = chunkStart; i < end; i++) {
                                ChiVertex<VertexDataType, EdgeDataType> vertex = vertices[i];
                                if (vertex != null && vertex.parallelSafe) {
                                    thrupdates++;
                                    program.update(vertex, threadContext);
                                }
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            int pending = countDown.decrementAndGet();
                            synchronized (termlock) {
                                nupdates += thrupdates;
                                if (pending == 0) {
                                    termlock.notifyAll();
                                }
                            }
                        }
                    }
                });
            }
            synchronized (termlock) {
                while(countDown.get() > 0) {
                    try {
                        termlock.wait(1500);
                    } catch (InterruptedException e) {
                        // What to do?
                        e.printStackTrace();
                    }
                    if (countDown.get() > 0) logger.info("Waiting for execution to finish: countDown:" + countDown.get());
                }
            }

        }
        _timer.stop();
    }

    protected int initVertices(int nvertices, int firstVertexId, ChiVertex<VertexDataType, EdgeDataType>[] vertices) throws IOException
    {
        final TimerContext _timer = initVerticesTimer.time();
        ChiVertex.edgeValueConverter = edataConverter;
        ChiVertex.vertexValueConverter = vertexDataConverter;
        ChiVertex.blockManager = blockManager;

        int blockId = (vertexDataConverter != null ? vertexDataHandler.load(firstVertexId, firstVertexId + nvertices - 1) : -1);
        for(int j=0; j < nvertices; j++) {
            if (enableScheduler && !scheduler.isScheduled(j + firstVertexId)) {
                continue;
            }

            VertexDegree degree = degreeHandler.getDegree(j + firstVertexId);
            if (skipZeroDegreeVertices && (degree.inDegree + degree.outDegree == 0)) {
                continue;
            }

            ChiVertex<VertexDataType, EdgeDataType> v = new ChiVertex<VertexDataType, EdgeDataType>(j + firstVertexId, degree);

            if (vertexDataConverter != null) {
                v.setDataPtr(vertexDataHandler.getVertexValuePtr(j + firstVertexId, blockId));
            }
            vertices[j] = v;
        }


        _timer.stop();
        return blockId;
    }

    private void loadBeforeUpdates(int interval, final ChiVertex<VertexDataType, EdgeDataType>[] vertices,  final MemoryShard<EdgeDataType> memShard,
                                   final int startVertex, final int endVertex) throws IOException {
        final Object terminationLock = new Object();
        final TimerContext _timer = loadTimer.time();
        // TODO: make easier to read
        synchronized (terminationLock) {

            final AtomicInteger countDown = new AtomicInteger(disableOutEdges ? 1 : nShards);

            if (!disableInEdges) {
                loadingExecutor.submit(new Runnable() {

                    public void run() {
                        try {
                            logger.info("Loading memshard started. " + Thread.currentThread().getName() + " id=" +
                                    Thread.currentThread().getId());
                            logger.info("Memshard:  " + memShard + " ... st: " + startVertex + " -- " + endVertex);
                            logger.info("Vertices length: " + vertices.length + "; disableOut=" + disableOutEdges);
                            memShard.loadVertices(startVertex, endVertex, vertices, disableOutEdges);
                            logger.info("Loading memshard finished." + Thread.currentThread().getName());

                            if (countDown.decrementAndGet() == 0) {
                                synchronized (terminationLock) {
                                    terminationLock.notifyAll();
                                }
                            }
                            logger.info("Finishing memshard task: " + Thread.currentThread().getName());

                        } catch (IOException ioe) {
                            ioe.printStackTrace();
                            throw new RuntimeException(ioe);
                        }  catch (Exception err) {
                            err.printStackTrace();
                        }
                    }
                });
            }

            /* Load in parallel */
            if (!disableOutEdges) {
                for(int p=0; p < nShards; p++) {
                    if (p != interval || disableInEdges) {
                        final int _p = p;
                        final SlidingShard<EdgeDataType> shard = slidingShards.get(p);
                        loadingExecutor.submit(new Runnable() {

                            public void run() {
                                try {
                                    shard.readNextVertices(vertices, startVertex, false);
                                    if (countDown.decrementAndGet() == 0) {
                                        synchronized (terminationLock) {
                                            terminationLock.notifyAll();
                                        }
                                    }

                                } catch (IOException ioe) {
                                    ioe.printStackTrace();
                                    throw new RuntimeException(ioe);
                                }  catch (Exception err) {
                                    err.printStackTrace();
                                }
                            }
                        });
                    }
                }
            }

            // barrier
            try {
                while(countDown.get() > 0) {
                    terminationLock.wait(5000);
                    if (countDown.get() > 0) {
                        logger.info("Still waiting for loading, counter is: " + countDown.get());
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        _timer.stop();
    }

    public GraphChiContext getContext() {
        return chiContext;
    }

    class IntervalData {
        private VertexInterval subInterval;
        private ChiVertex<VertexDataType, EdgeDataType>[] vertices;
        private int vertexBlockId;
        private MemoryShard<EdgeDataType> memShard;
        private int intervalNum;

        IntervalData(VertexInterval subInterval, ChiVertex<VertexDataType, EdgeDataType>[] vertices, int vertexBlockId,
                     MemoryShard<EdgeDataType> memShard, int intervalNum) {
            this.subInterval = subInterval;
            this.vertices = vertices;
            this.vertexBlockId = vertexBlockId;
            this.intervalNum = intervalNum;
            this.memShard = memShard;
        }

        public VertexInterval getSubInterval() {
            return subInterval;
        }

        public ChiVertex<VertexDataType, EdgeDataType>[] getVertices() {
            return vertices;
        }

        public int getVertexBlockId() {
            return vertexBlockId;
        }

        public MemoryShard<EdgeDataType> getMemShard() {
            return memShard;
        }

        public int getIntervalNum() {
            return intervalNum;
        }
    }

    class AutoLoaderTask implements Callable<IntervalData> {

        private ChiVertex<VertexDataType, EdgeDataType>[] vertices;
        private VertexInterval interval;
        private MemoryShard<EdgeDataType> memShard;
        private int intervalNum;

        AutoLoaderTask(VertexInterval interval, int intervalNum, MemoryShard<EdgeDataType> memShard) {
            this.interval = interval;
            this.memShard = memShard;
            this.intervalNum = intervalNum;
            if (!onlyAdjacency)  throw new RuntimeException("Can use auto-loading only with only-adjacency mode!");
        }

        @Override
        public IntervalData call() {
            try {
                int lastVertex  = determineNextWindow(interval.getFirstVertex(), interval.getLastVertex());
                int nVertices = lastVertex - interval.getFirstVertex() + 1;
                this.vertices = (ChiVertex<VertexDataType, EdgeDataType>[]) new ChiVertex[nVertices];
                int vertexBlockid = initVertices(nVertices, interval.getFirstVertex(), vertices);

                loadBeforeUpdates(intervalNum, vertices, memShard, interval.getFirstVertex(), lastVertex);
                return new IntervalData(new VertexInterval(interval.getFirstVertex(), lastVertex), vertices, vertexBlockid, memShard, intervalNum);

            } catch (NoEdgesInIntervalException nie) {
                return new IntervalData(new VertexInterval(interval.getFirstVertex(), interval.getLastVertex()), vertices, -1, memShard, intervalNum);

            } catch (Exception err) {
                err.printStackTrace();
                return null;
            }
        }


    }

    private int determineNextWindow(int subIntervalStart, int maxVertex) throws IOException, NoEdgesInIntervalException {
        final TimerContext _timer = determineNextWindowTimer.time();
        long totalDegree = 0;
        try {
            degreeHandler.load(subIntervalStart, maxVertex);

            if (useStaticWindowSize) {
                return maxVertex;
            }

            long memReq = 0;
            int maxInterval = maxVertex - subIntervalStart;
            int vertexDataSizeOf = (vertexDataConverter != null ? vertexDataConverter.sizeOf() : 0);
            int edataSizeOf = (onlyAdjacency ? 0 : edataConverter.sizeOf());

            for(int i=0; i< maxInterval; i++) {
                if (enableScheduler) {
                    if (!scheduler.isScheduled(i + subIntervalStart)) continue;
                }
                VertexDegree deg = degreeHandler.getDegree(i + subIntervalStart);
                int inc = deg.inDegree;
                int outc = deg.outDegree;

                if (inc + outc == 0 && skipZeroDegreeVertices) {
                    continue;
                }

                totalDegree += inc + outc;

                // Following calculation contains some perhaps reasonable estimates of the
                // overhead of Java objects.
                memReq += vertexDataSizeOf + 256 + (edataSizeOf + 4 + 4 + 4) * (inc + outc);
                if (memReq > memBudget) {
                    if (totalDegree == 0 && vertexDataConverter == null) {
                        throw new NoEdgesInIntervalException();
                    }
                    return subIntervalStart + i - 1; // Previous vertex was enough
                }
            }
            if (totalDegree == 0 && vertexDataConverter == null) {
                throw new NoEdgesInIntervalException();
            }
            return maxVertex;
        } finally {
            _timer.stop();
        }
    }

    public boolean isEnableScheduler() {
        return enableScheduler;
    }

    public void setEnableScheduler(boolean enableScheduler) {
        this.enableScheduler = enableScheduler;
    }

    public void setVertexDataConverter(BytesToValueConverter<VertexDataType> vertexDataConverter) {
        this.vertexDataConverter = vertexDataConverter;
    }

    public void setEdataConverter(BytesToValueConverter<EdgeDataType> edataConverter) {
        this.edataConverter = edataConverter;
    }

    public boolean isEnableDeterministicExecution() {
        return enableDeterministicExecution;
    }

    public void setEnableDeterministicExecution(boolean enableDeterministicExecution) {
        this.enableDeterministicExecution = enableDeterministicExecution;
    }

    public boolean isDisableOutEdges() {
        return disableOutEdges;
    }

    public void setDisableOutEdges(boolean disableOutEdges) {
        this.disableOutEdges = disableOutEdges;
    }

    public boolean isModifiesInedges() {
        return modifiesInedges;
    }

    public void setModifiesInedges(boolean modifiesInedges) {
        this.modifiesInedges = modifiesInedges;
    }

    public boolean isModifiesOutedges() {
        return modifiesOutedges;
    }

    public void setModifiesOutedges(boolean modifiesOutedges) {
        this.modifiesOutedges = modifiesOutedges;
    }

    public boolean isOnlyAdjacency() {
        return onlyAdjacency;
    }

    public void setOnlyAdjacency(boolean onlyAdjacency) {
        this.onlyAdjacency = onlyAdjacency;
    }

    public void setDisableInedges(boolean b) {
        this.disableInEdges = b;
    }

    public int getMaxWindow() {
        return maxWindow;
    }

    public void setMaxWindow(int maxWindow) {
        this.maxWindow = maxWindow;
    }

    public boolean isUseStaticWindowSize() {
        return useStaticWindowSize;
    }

    public void setUseStaticWindowSize(boolean useStaticWindowSize) {
        this.useStaticWindowSize = useStaticWindowSize;
    }

    public boolean isAutoLoadNext() {
        return autoLoadNext;
    }

    public void setAutoLoadNext(boolean autoLoadNext) {
        this.autoLoadNext = autoLoadNext;
    }

    private class MockScheduler implements Scheduler {

        public void addTask(int vertexId) {

        }

        public void removeTasks(int from, int to) {
        }

        public void addAllTasks() {

        }

        public boolean hasTasks() {
            return true;
        }

        public boolean isScheduled(int i) {
            return true;
        }

        @Override
        public void removeAllTasks() {

        }

        @Override
        public void scheduleOutNeighbors(ChiVertex vertex) {
        }

        @Override
        public void scheduleInNeighbors(ChiVertex vertex) {
        }
    }

    public VertexIdTranslate getVertexIdTranslate() {
        return vertexIdTranslate;
    }

    public void setVertexIdTranslate(VertexIdTranslate vertexIdTranslate) {
        this.vertexIdTranslate = vertexIdTranslate;
    }
}

class NoEdgesInIntervalException extends Exception {


}