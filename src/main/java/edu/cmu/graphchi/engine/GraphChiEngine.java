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
import edu.cmu.graphchi.shards.MemoryShard;
import edu.cmu.graphchi.shards.SlidingShard;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
    protected MemoryShard<EdgeDataType> memoryShard;

    protected BytesToValueConverter<EdgeDataType> edataConverter;
    protected BytesToValueConverter<VertexDataType> vertexDataConverter;

    protected int execInterval;
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

    /* Automatic loading of next window */
    private boolean autoLoadNext = false; // Only for only-adjacency cases!
    private FutureTask<IntervalData> nextWindow;

    /* Metrics */
    private final Timer loadTimer = Metrics.newTimer(GraphChiEngine.class, "shard-loading", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer executionTimer = Metrics.newTimer(GraphChiEngine.class, "execute-updates", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer waitForFutureTimer = Metrics.newTimer(GraphChiEngine.class, "wait-for-future", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer initVerticesTimer = Metrics.newTimer(GraphChiEngine.class, "init-vertices", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer determineNextWindowTimer = Metrics.newTimer(GraphChiEngine.class, "det-next-window", TimeUnit.SECONDS, TimeUnit.MINUTES);


    protected boolean modifiesInedges = true, modifiesOutedges = true;
    private boolean disableInEdges = false;

    public GraphChiEngine(String baseFilename, int nShards) throws FileNotFoundException, IOException {
        this.baseFilename = baseFilename;
        this.nShards = nShards;
        loadIntervals();

        int nprocs = 4;
        if (Runtime.getRuntime().availableProcessors() > nprocs) {
            nprocs = Runtime.getRuntime().availableProcessors();
        }
        parallelExecutor = Executors.newFixedThreadPool(nprocs);
        loadingExecutor = Executors.newFixedThreadPool(4);
        blockManager = new DataBlockManager();
        degreeHandler = new DegreeData(baseFilename);

        memBudget = Runtime.getRuntime().maxMemory() / 4;
        if (Runtime.getRuntime().maxMemory() < 256 * 1024 * 1024)
            throw new IllegalArgumentException("Java Virtual Machine has only " + memBudget + "bytes maximum memory." +
                    " Please run the JVM with at least 256 megabytes of memory using -Xmx256m. For better performance, use higher value");

    }


    protected void loadIntervals() throws FileNotFoundException, IOException {
        String intervalFilename = ChiFilenames.getFilenameIntervals(baseFilename, nShards);

        BufferedReader rd = new BufferedReader(new FileReader(new File(intervalFilename)));
        String line;
        int lastId = 0;
        intervals = new ArrayList<VertexInterval>(nShards);
        while((line = rd.readLine()) != null) {
            int vid = Integer.parseInt(line);
            intervals.add(new VertexInterval(lastId, vid));
            lastId = vid + 1;
        }

        assert(intervals.size() == nShards);

        System.out.println("Loaded: " + intervals);
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

    protected void createMemoryShard(int intervalStart, int intervalEnd) {
        String edataFilename = (onlyAdjacency ? null : ChiFilenames.getFilenameShardEdata(baseFilename, edataConverter, execInterval, nShards));
        String adjFilename = ChiFilenames.getFilenameShardsAdj(baseFilename, execInterval, nShards);

        memoryShard = new MemoryShard<EdgeDataType>(edataFilename, adjFilename, intervals.get(execInterval).getFirstVertex(),
                intervals.get(execInterval).getLastVertex());
        memoryShard.setConverter(edataConverter);
        memoryShard.setDataBlockManager(blockManager);
        memoryShard.setOnlyAdjacency(onlyAdjacency);
    }


    public void run(GraphChiProgram<VertexDataType, EdgeDataType> program, int niters) throws IOException {
        chiContext.setNumIterations(niters);

        long startTime = System.currentTimeMillis();
        initializeSlidingShards();

        if (enableScheduler) {
            initializeScheduler();
            chiContext.setScheduler(scheduler);
            scheduler.addAllTasks();
            System.out.println("Using scheduler!");
        }  else {
            chiContext.setScheduler(new MockScheduler());
        }


        if (disableInEdges) {
            ChiVertex.disableInedges = true;
        }

        /* Initialize vertex-data handler */
        vertexDataHandler = new VertexData<VertexDataType>(numVertices(), baseFilename, vertexDataConverter);
        vertexDataHandler.setBlockManager(blockManager);

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
                    System.out.println("No new tasks to run. Terminating.");
                    break;
                }
                scheduler.reset();
            }

            for(execInterval=0; execInterval < nShards; ++execInterval) {
                int intervalSt = intervals.get(execInterval).getFirstVertex();
                int intervalEn = intervals.get(execInterval).getLastVertex();

                System.out.println((System.currentTimeMillis() - startTime) * 0.001 + "s: iteration: " + iter + ", interval: " + intervalSt + " -- " + intervalEn);

                program.beginInterval(chiContext, intervals.get(execInterval));

                if (!disableInEdges) {
                    slidingShards.get(execInterval).flush();
                    createMemoryShard(intervalSt, intervalEn);
                }

                subIntervalStart = intervalSt;

                while (subIntervalStart <= intervalEn) {
                    if (anyVertexScheduled(subIntervalStart, Math.min(intervalEn, subIntervalStart + maxWindow ))) {
                        ChiVertex<VertexDataType, EdgeDataType>[] vertices = null;
                        int vertexBlockId = -1;

                        if (!autoLoadNext || nextWindow == null) {
                            subIntervalEnd = determineNextWindow(subIntervalStart, Math.min(intervalEn, subIntervalStart + maxWindow ));
                            int nvertices = subIntervalEnd - subIntervalStart + 1;

                            System.out.println("Subinterval:: " + subIntervalStart + " -- " + subIntervalEnd + " (iteration " + iter + ")");

                            vertices = new ChiVertex[nvertices];


                            System.out.println("Init vertices...");
                            vertexBlockId = initVertices(nvertices, subIntervalStart, vertices);

                            System.out.println("Loading...");
                            long t0 = System.currentTimeMillis();
                            loadBeforeUpdates(vertices, subIntervalStart, subIntervalEnd);
                            System.out.println("Load took: " + (System.currentTimeMillis() - t0) + "ms");
                        } else {
                            /* This is a mess! */
                            try {
                                long tf = System.currentTimeMillis();
                                final TimerContext _timer = waitForFutureTimer.time();
                                IntervalData next = nextWindow.get();
                                _timer.stop();
                                System.out.println("Waiting for future task loading took " + (System.currentTimeMillis() - tf) + " ms");
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
                            if (subIntervalEnd + 1 <= intervalEn) {
                                nextWindow = new FutureTask<IntervalData>(new AutoLoaderTask(new VertexInterval(subIntervalEnd + 1,
                                        Math.min(intervalEn, subIntervalEnd + 1 + maxWindow))));
                                parallelExecutor.submit(nextWindow);
                            }
                        }
                        /* Clear scheduler bits */
                        if (scheduler != null) scheduler.removeTasks(subIntervalStart, subIntervalEnd);

                        program.beginSubInterval(chiContext, new VertexInterval(subIntervalStart, subIntervalEnd));

                        long t1 = System.currentTimeMillis();
                        execUpdates(program, vertices);
                        System.out.println("Update exec: " + (System.currentTimeMillis() - t1) + " ms.");

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
                        subIntervalStart = subIntervalEnd + 1;
                        System.out.println("Skipped interval - no vertices scheduled.");
                    }
                }


                /* Commit */
                if (!disableInEdges) {
                    memoryShard.commitAndRelease(modifiesInedges, modifiesOutedges);
                    slidingShards.get(execInterval).setOffset(memoryShard.getStreamingOffset(),
                            memoryShard.getStreamingOffsetVid(), memoryShard.getStreamingOffsetEdgePtr());
                }
            }

            for(SlidingShard shard : slidingShards) {
                shard.flush();
                shard.setOffset(0, 0, 0);
            }
        }    // Iterations

        parallelExecutor.shutdown();
        loadingExecutor.shutdown();
        System.out.println("Engine finished in: " + (System.currentTimeMillis() - startTime) * 0.001 + " secs.");
        System.out.println("Updates: " + nupdates);
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
            final int chunkSize = vertices.length / 64;

            final int nWorkers = vertices.length / chunkSize + 1;
            final AtomicInteger countDown = new AtomicInteger(1 + nWorkers);

            if (!enableDeterministicExecution) {
                for(ChiVertex<VertexDataType, EdgeDataType> vertex : vertices) {
                    if (vertex != null) vertex.parallelSafe = true;
                }
            }

            synchronized (termlock) {
                /* Parallel updates. One thread for non-parallel safe updates, others
             updated in parallel. This guarantees deterministic execution. */

                /* Non-safe updates */
                parallelExecutor.submit(new Runnable() {
                    public void run() {
                        int thrupdates = 0;
                        try {
                            for(ChiVertex<VertexDataType, EdgeDataType> vertex : vertices) {
                                if (vertex != null && !vertex.parallelSafe) {
                                    thrupdates++;
                                    program.update(vertex, chiContext);
                                }
                            }
                            int pending = countDown.decrementAndGet();
                            synchronized (termlock) {
                                nupdates += thrupdates;
                                if (pending == 0) {
                                    termlock.notifyAll();;
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
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
                            try {
                                int end = chunkEnd;
                                if (end > vertices.length) end = vertices.length;
                                for(int i = chunkStart; i < end; i++) {
                                    ChiVertex<VertexDataType, EdgeDataType> vertex = vertices[i];
                                    if (vertex != null && vertex.parallelSafe) {
                                        thrupdates++;
                                        program.update(vertex, chiContext);
                                    }
                                }
                                int pending = countDown.decrementAndGet();
                                synchronized (termlock) {
                                    nupdates += thrupdates;
                                    if (pending == 0) {
                                        termlock.notifyAll();;
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                        }
                    });
                }

                while(countDown.get() > 0) {
                    try {
                        termlock.wait(500);
                    } catch (InterruptedException e) {
                        // What to do?
                        e.printStackTrace();
                    }
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

        int blockId = vertexDataHandler.load(firstVertexId, firstVertexId + nvertices - 1);
        for(int j=0; j < nvertices; j++) {
            VertexDegree degree = degreeHandler.getDegree(j + firstVertexId);

            if (enableScheduler && !scheduler.isScheduled(j + firstVertexId)) {
                continue;
            }
            ChiVertex<VertexDataType, EdgeDataType> v = new ChiVertex<VertexDataType, EdgeDataType>(j + firstVertexId, degree);
            v.setDataPtr(vertexDataHandler.getVertexValuePtr(j + firstVertexId, blockId));
            vertices[j] = v;

            if (j % 200000 == 0)
                System.out.println("Init progress: " + j + " / " + nvertices);
        }
        _timer.stop();
        return blockId;
    }

    private void loadBeforeUpdates(final ChiVertex<VertexDataType, EdgeDataType>[] vertices,
                                   final int startVertex, final int endVertex) throws IOException {
        final Object terminationLock = new Object();
        final TimerContext _timer = loadTimer.time();
        // TODO: make easier to read
        synchronized (terminationLock) {

            final AtomicInteger countDown = new AtomicInteger(nShards);
            /* Load in parallel */
            for(int p=0; p < nShards; p++) {
                if (p != execInterval || disableInEdges) {
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
            if (!disableInEdges) {
                loadingExecutor.submit(new Runnable() {

                    public void run() {
                        try {
                            memoryShard.loadVertices(startVertex, endVertex, vertices);
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
            // barrier
            try {
                while(countDown.get() > 0) {
                    terminationLock.wait();

                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        _timer.stop();
    }

    class IntervalData {
        private VertexInterval subInterval;
        private ChiVertex<VertexDataType, EdgeDataType>[] vertices;
        private int vertexBlockId;

        IntervalData(VertexInterval subInterval, ChiVertex<VertexDataType, EdgeDataType>[] vertices, int vertexBlockId) {
            this.subInterval = subInterval;
            this.vertices = vertices;
            this.vertexBlockId = vertexBlockId;
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
    }

    class AutoLoaderTask implements Callable<IntervalData> {

        private ChiVertex<VertexDataType, EdgeDataType>[] vertices;
        private VertexInterval interval;

        AutoLoaderTask(VertexInterval interval) {
            this.interval = interval;
            if (!onlyAdjacency)  throw new RuntimeException("Can use auto-loading only with only-adjacency mode!");
        }

        @Override
        public IntervalData call() {
            try {
                int lastVertex  = determineNextWindow(interval.getFirstVertex(), interval.getLastVertex());
                int nVertices = lastVertex - interval.getFirstVertex() + 1;
                this.vertices = (ChiVertex<VertexDataType, EdgeDataType>[]) new ChiVertex[nVertices];

                int vertexBlockid = initVertices(nVertices, interval.getFirstVertex(), vertices);

                loadBeforeUpdates(vertices, interval.getFirstVertex(), lastVertex);
                return new IntervalData(new VertexInterval(interval.getFirstVertex(), lastVertex), vertices, vertexBlockid);
            } catch (Exception err) {
                err.printStackTrace();
                return null;
            }
        }
    }

    private int determineNextWindow(int subIntervalStart, int maxVertex) throws IOException {
        final TimerContext _timer = determineNextWindowTimer.time();
        try {
            degreeHandler.load(subIntervalStart, maxVertex);

            if (useStaticWindowSize) {
                return maxVertex;
            }

            long memReq = 0;
            int maxInterval = maxVertex - subIntervalStart;
            System.out.println("mem budget: " + memBudget / 1024. / 1024. + "mb");
            int vertexDataSizeOf = vertexDataConverter.sizeOf();
            int edataSizeOf = (onlyAdjacency ? 0 : edataConverter.sizeOf());

            for(int i=0; i< maxInterval; i++) {
                if (enableScheduler) {
                    if (!scheduler.isScheduled(i + subIntervalStart)) continue;
                }
                VertexDegree deg = degreeHandler.getDegree(i + subIntervalStart);
                int inc = deg.inDegree;
                int outc = deg.outDegree;

                // Following calculation contains some perhaps reasonable estimates of the
                // overhead of Java objects.
                memReq += vertexDataSizeOf + 256 + (edataSizeOf + 4 + 4 + 4) * (inc + outc);
                if (memReq > memBudget) {
                    return subIntervalStart + i - 1; // Previous vertex was enough
                }
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
    }
}
