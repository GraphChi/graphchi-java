package edu.cmu.graphchi.engine;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.apps.Pagerank;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.engine.auxdata.DegreeData;
import edu.cmu.graphchi.engine.auxdata.VertexDegree;
import edu.cmu.graphchi.shards.MemoryShard;
import edu.cmu.graphchi.shards.SlidingShard;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
    private DegreeData degreeHandler;

    protected int subIntervalStart, subIntervalEnd;

    public GraphChiEngine(String baseFilename, int nShards) throws FileNotFoundException, IOException {
        this.baseFilename = baseFilename;
        this.nShards = nShards;
        loadIntervals();

        parallelExecutor = Executors.newFixedThreadPool(4);
        blockManager = new DataBlockManager();
        degreeHandler = new DegreeData(baseFilename);
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


    public int numVertices() {
        return 1 + intervals.get(intervals.size() - 1).getLastVertex();
    }

    protected void initializeSlidingShards() throws IOException {
        slidingShards = new ArrayList<SlidingShard<EdgeDataType> >();
        for(int p=0; p < nShards; p++) {
            String edataFilename = ChiFilenames.getFilenameShardEdata(baseFilename, edataConverter, p, nShards);
            String adjFilename = ChiFilenames.getFilenameShardsAdj(baseFilename, p, nShards);

            SlidingShard<EdgeDataType> slidingShard = new SlidingShard<EdgeDataType>(edataFilename, adjFilename, intervals.get(p).getFirstVertex(),
                    intervals.get(p).getLastVertex());
            slidingShard.setConverter(edataConverter);
            slidingShard.setDataBlockManager(blockManager);
            slidingShards.add(slidingShard);

        }
    }

    protected void createMemoryShard(int intervalStart, int intervalEnd) {
        String edataFilename = ChiFilenames.getFilenameShardEdata(baseFilename, edataConverter, execInterval, nShards);
        String adjFilename = ChiFilenames.getFilenameShardsAdj(baseFilename, execInterval, nShards);

        memoryShard = new MemoryShard<EdgeDataType>(edataFilename, adjFilename, intervals.get(execInterval).getFirstVertex(),
                intervals.get(execInterval).getLastVertex());
        memoryShard.setConverter(edataConverter);
        memoryShard.setDataBlockManager(blockManager);
    }


    public void run(GraphChiProgram<VertexDataType, EdgeDataType> program, int niters) throws IOException {
        chiContext.setNumIterations(niters);

        long startTime = System.currentTimeMillis();
        initializeSlidingShards();

        /* Temporary: keep vertices in memory */
        int vertexBlockId = blockManager.allocateBlock(numVertices() * vertexDataConverter.sizeOf());
        int maxWindow = 20000000;

        for(int iter=0; iter < niters; iter++) {
            chiContext.setIteration(iter);
            chiContext.setNumVertices(numVertices());
            program.beginIteration(chiContext);

            for(execInterval=0; execInterval < nShards; ++execInterval) {
                int intervalSt = intervals.get(execInterval).getFirstVertex();
                int intervalEn = intervals.get(execInterval).getLastVertex();

                System.out.println((System.currentTimeMillis() - startTime) * 0.001 + "s: iteration: " + iter + ", sub-interval: " + intervalSt + " -- " + intervalEn);

                program.beginInterval(chiContext, intervals.get(execInterval));

                slidingShards.get(execInterval).flush();

                createMemoryShard(intervalSt, intervalEn);

                subIntervalStart = intervalSt;

                while (subIntervalStart < intervalEn) {
                    subIntervalEnd = determineNextWindow(subIntervalStart, Math.min(intervalEn, subIntervalStart + maxWindow ));
                    int nvertices = subIntervalEnd - subIntervalStart + 1;

                    System.out.println("Subinterval:: " + subIntervalStart + " -- " + subIntervalEnd + " (iteration " + iter + ")");


                    ChiVertex<VertexDataType, EdgeDataType>[] vertices = new
                            ChiVertex[nvertices];

                    System.out.println("Init vertices...");
                    initVertices(vertexBlockId, nvertices, vertices);

                    System.out.println("Loading...");
                    loadBeforeUpdates(vertices);

                    execUpdates(program, vertices);

                    subIntervalStart = subIntervalEnd + 1;
                }


                /* Commit */
                memoryShard.commitAndRelease();
                slidingShards.get(execInterval).setOffset(memoryShard.getRangeStartOffset(),
                        memoryShard.getRangeContVid(), memoryShard.getRangeStartEdgePtr());
            }

            for(SlidingShard shard : slidingShards) {
                shard.flush();
                shard.setOffset(0, 0, 0);
            }
        }    // Iterations

        parallelExecutor.shutdown();
        System.out.println("Engine finished in: " + (System.currentTimeMillis() - startTime) * 0.001 + " secs.");
    }

    private void execUpdates(GraphChiProgram<VertexDataType, EdgeDataType> program, ChiVertex<VertexDataType, EdgeDataType>[] vertices) {
        System.out.println("---- Executing updates ---");
        for(ChiVertex<VertexDataType, EdgeDataType> vertex : vertices) {
            program.update(vertex, chiContext);
        }
    }

    protected void initVertices(int vertexBlockId, int nvertices, ChiVertex<VertexDataType, EdgeDataType>[] vertices)
    {
        ChiVertex.edgeValueConverter = edataConverter;
        ChiVertex.vertexValueConverter = vertexDataConverter;
        ChiVertex.blockManager = blockManager;
        for(int j=0; j < nvertices; j++) {
            VertexDegree degree = degreeHandler.getDegree(j + subIntervalStart);
            ChiVertex<VertexDataType, EdgeDataType> v = new ChiVertex<VertexDataType, EdgeDataType>(j + subIntervalStart, degree);
            v.setDataPtr(new ChiPointer(vertexBlockId, (int) v.getId() * vertexDataConverter.sizeOf()));
            vertices[j] = v;

            if (j % 200000 == 0)
                System.out.println("Init progress: " + j + " / " + nvertices);
        }
    }

    private void loadBeforeUpdates(final ChiVertex<VertexDataType, EdgeDataType>[] vertices) throws IOException {
        final Object terminationLock = new Object();

        // TODO: make easier to read
        synchronized (terminationLock) {

            final AtomicInteger countDown = new AtomicInteger(nShards);
            /* Load in parallel */
            for(int p=0; p < nShards; p++) {
                if (p != execInterval) {
                    final SlidingShard<EdgeDataType> shard = slidingShards.get(p);
                    parallelExecutor.submit(new Runnable() {

                        public void run() {
                            try {
                                shard.readNextVertices(vertices, subIntervalStart, false);
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
            parallelExecutor.submit(new Runnable() {

                public void run() {
                    try {
                        memoryShard.loadVertices(subIntervalStart, subIntervalEnd, vertices);
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
            // barrier
            try {
                while(countDown.get() > 0) {
                    terminationLock.wait();

                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private int determineNextWindow(int subIntervalStart, int maxVertex) throws IOException {
        // TODO
        degreeHandler.load(subIntervalStart, maxVertex);
        long memReq = 0;
        long memBudget = Runtime.getRuntime().maxMemory() / 4;
        if (Runtime.getRuntime().maxMemory() < 256 * 1024 * 1024)
            throw new IllegalArgumentException("Java Virtual Machine has only " + memBudget + "bytes maximum memory." +
                    " Please run the JVM with at least 256 megabytes of memory using -Xmx256m. For better performance, use higher value");
        int maxInterval = maxVertex - subIntervalStart;
        System.out.println("mem budget: " + memBudget / 1024. / 1024. + "mb");
        int vertexDataSizeOf = vertexDataConverter.sizeOf();
        int edataSizeOf = edataConverter.sizeOf();

        for(int i=0; i< maxInterval; i++) {
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
    }

    public void setVertexDataConverter(BytesToValueConverter<VertexDataType> vertexDataConverter) {
        this.vertexDataConverter = vertexDataConverter;
    }

    public void setEdataConverter(BytesToValueConverter<EdgeDataType> edataConverter) {
        this.edataConverter = edataConverter;
    }
}
