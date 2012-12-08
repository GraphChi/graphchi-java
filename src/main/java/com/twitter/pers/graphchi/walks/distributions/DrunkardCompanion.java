package com.twitter.pers.graphchi.walks.distributions;

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import com.twitter.pers.graphchi.walks.WalkManager;
import edu.cmu.graphchi.LoggingInitializer;
import edu.cmu.graphchi.util.IdCount;
import edu.cmu.graphchi.util.IntegerBuffer;

import java.io.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * DrunkardCompanion is a remote service that receives walks from the walker
 * and maintains a distribution from each source.
 */
public class DrunkardCompanion extends UnicastRemoteObject implements RemoteDrunkardCompanion {

    private static class WalkSubmission {
        int[] walks;
        int[] atVertices;

        private WalkSubmission(int[] walks, int[] atVertices) {
            this.walks = walks;
            this.atVertices = atVertices;
        }
    }

    private static final int BUFFER_CAPACITY = 128;
    private static final int BUFFER_MAX = 128;

    private int[] sourceVertexIds;
    private Object[] distrLocks;
    boolean isLowInMemory = false;

    private DiscreteDistribution[] distributions;
    private IntegerBuffer[] buffers;
    private AtomicInteger outstanding = new AtomicInteger(0);

    private ExecutorService parallelExecutor;
    private Double pruneFraction;
    private String workingDir;
    private LinkedBlockingQueue<WalkSubmission> pendingQueue = new LinkedBlockingQueue<WalkSubmission>();

    private static Logger logger = LoggingInitializer.getLogger("drunkardcompanion");
    private Timer timer  = new Timer(true);

    /**
     * Prints estimate of memory usage
     */
    private long memoryAuditReport() {
        long companionOverHeads = 0;
        companionOverHeads += sourceVertexIds.length * 4;
        companionOverHeads += distrLocks.length * 4;


        long bufferMem = 0;
        long maxMem = 0;
        for(IntegerBuffer buf : buffers) {
            long est = buf.memorySizeEst();
            bufferMem += est;
            maxMem = Math.max(maxMem, est);
        }

        long distributionMem = 0;
        long maxDistMem = 0;
        long avoidMem = 0;
        for(DiscreteDistribution dist : distributions) {
            long est = dist.memorySizeEst();
            distributionMem += est;
            maxDistMem = Math.max(est, maxDistMem);
            avoidMem += dist.avoidCount() * 6;
        }

        NumberFormat nf = NumberFormat.getInstance(Locale.US);

        logger.info("======= MEMORY REPORT ======");
        logger.info("Companion internal: " + nf.format(companionOverHeads / 1024. / 1024.) + " mb");

        logger.info("Buffer mem: " + nf.format(bufferMem / 1024. / 1024.) + " mb");
        logger.info("Avg bytes per buffer: " + nf.format(bufferMem * 1.0 / buffers.length / 1024.) + " kb");
        logger.info("Max buffer was: " + nf.format(maxMem / 1024.) + "kb");

        logger.info("Distribution mem: " + nf.format(distributionMem / 1024. / 1024.) + " mb");
        logger.info("- of which avoids: " + nf.format(avoidMem / 1024. / 1024.) + " mb");

        logger.info("Avg bytes per distribution: " + nf.format((distributionMem * 1.0 / distributions.length / 1024.)) + " kb");
        logger.info("Max distribution: " + nf.format(maxDistMem / 1024.) + " kb");

        long totalMem = companionOverHeads + bufferMem + distributionMem;
        logger.info("** Total:  " + nf.format(totalMem / 1024. / 1024. / 1024.) + " GB (low-mem limit " + Runtime.getRuntime().maxMemory() * 0.75 / 1024. / 1024. / 1024. + "GB)" );
        isLowInMemory = totalMem > Runtime.getRuntime().maxMemory() * 0.75;


        return totalMem;
    }



    public DrunkardCompanion(double pruneFraction, String workingDir) throws RemoteException {
        parallelExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.pruneFraction = pruneFraction;
        this.workingDir = workingDir;

        final int numThreads = 4;

        for(int threadId=0; threadId < numThreads; threadId++) {
            final int _threadId = threadId;
            Thread processingThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {

                        long unpurgedWalks = 0;
                        while(true) {

                            WalkSubmission subm = pendingQueue.poll(2000, TimeUnit.MILLISECONDS);
                            if (subm != null) {
                                _processWalks(subm.walks, subm.atVertices);
                                unpurgedWalks += subm.walks.length;
                            }
                            if (sourceVertexIds != null) {
                                if (unpurgedWalks > sourceVertexIds.length * 10 || (subm == null && unpurgedWalks > 100000)) {
                                    logger.fine("Purge:" + unpurgedWalks);
                                    unpurgedWalks = 0;

                                    // Loop to see what to drain. Every thread looks for
                                    // different buffers.
                                    for(int i=_threadId; i < sourceVertexIds.length; i+=numThreads) {
                                        if (buffers[i].size() >= BUFFER_MAX) {
                                            // Drain asynchronously
                                            outstanding.incrementAndGet();
                                            final IntegerBuffer toDrain = buffers[i];
                                            final int drainIdx = i;

                                            synchronized (buffers[i]) {
                                                buffers[i] = new IntegerBuffer(BUFFER_CAPACITY);
                                            }
                                            parallelExecutor.submit(new Runnable() { public void run() {
                                                try {
                                                    int[] d = toDrain.toIntArray();
                                                    Arrays.sort(d);
                                                    DiscreteDistribution dist = new DiscreteDistribution(d);
                                                    mergeWith(drainIdx, dist);
                                                } catch (Exception err ) {
                                                    err.printStackTrace();
                                                } finally {
                                                    outstanding.decrementAndGet();
                                                }
                                            }});
                                        }
                                    }
                                }
                            }

                        }
                    } catch (Exception err) {
                        err.printStackTrace();
                    }
                }
            });
            processingThread.start();
        }
    }

    private void mergeWith(int sourceIdx, DiscreteDistribution distr) {
        synchronized (distrLocks[sourceIdx]) {
            distributions[sourceIdx] = DiscreteDistribution.merge(distributions[sourceIdx], distr);

            if (pruneFraction > 0.0 && isLowInMemory) {
                int sz = distributions[sourceIdx].sizeExcludingAvoids();
                if (sz > 200) {
                    int mx = distributions[sourceIdx].max();
                    int pruneLimit = 2 + (int) (mx * pruneFraction);
                    DiscreteDistribution filtered =  distributions[sourceIdx].filteredAndShift((short)pruneLimit);
                    if (filtered.sizeExcludingAvoids() > 25) { // ad-hoc...
                        distributions[sourceIdx] = filtered;
                        int prunedSize = distributions[sourceIdx].sizeExcludingAvoids();
                        if (sourceIdx % 10000 == 0) {
                            logger.info("Pruned: " + sz + " => " + prunedSize + " max: " + mx + ", limit=" + pruneLimit);
                        }
                    } else {
                        //  logger.info("Filtering would have deleted almost everything...");
                        // Try pruning ones
                        filtered = distributions[sourceIdx].filteredAndShift((short)2);
                        if (filtered.sizeExcludingAvoids() > 25) {
                            distributions[sourceIdx] = filtered;
                        }  else {
                            distributions[sourceIdx] = distributions[sourceIdx].filteredAndShift((short)1);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void setAvoidList(int sourceIdx, int[] avoidList) throws RemoteException {
        Arrays.sort(avoidList);
        DiscreteDistribution avoidDistr = DiscreteDistribution.createAvoidanceDistribution(avoidList);
        mergeWith(sourceIdx, avoidDistr);
    }

    @Override
    public void setSources(int[] sources) throws RemoteException {
        // Restart timer
        timer.cancel();
        timer = new Timer(true);

        logger.info("Initializing sources...");
        buffers = new IntegerBuffer[sources.length];
        sourceVertexIds = new int[sources.length];
            distrLocks = new Object[sources.length];
        distributions = new DiscreteDistribution[sources.length];
        for(int i=0; i < sources.length; i++) {
            distrLocks[i] = new Object();
            sourceVertexIds[i] = sources[i];
            buffers[i] = new IntegerBuffer(BUFFER_CAPACITY);
            distributions[i] = DiscreteDistribution.createAvoidanceDistribution(new int[]{sources[i]}); // Add the vertex itself to avoids
        }
        logger.info("Done...");

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                memoryAuditReport();
            }
        }, 5000, 60000);
    }





    private void _processWalks(int[] walks, int[] atVertices) {
        long t1 = System.currentTimeMillis();
        for(int i=0; i < walks.length; i++) {
            int w = walks[i];
            int atVertex = atVertices[i];
            int sourceIdx = WalkManager.sourceIdx(w);

            if (atVertex == sourceVertexIds[sourceIdx]) {
                continue;
            }

            synchronized (buffers[sourceIdx]) {
                buffers[sourceIdx].add(atVertex);
            }
        }

        long tt = (System.currentTimeMillis() - t1);
        if (tt > 1000) {
            logger.info("Processing " + walks.length + " took " + tt + " ms.");
        }
    }

    @Override
    public IdCount[] getTop(int vertexId) throws RemoteException {
        int sourceIdx = (sourceVertexIds == null ? -1 : Arrays.binarySearch(sourceVertexIds, vertexId));
        if (sourceIdx >= 0) {
            logger.info("Found from memory: " + vertexId);
            logger.info("Buffer size:" + buffers[sourceIdx].size());
            int[] arr = buffers[sourceIdx].toIntArray();
            for(Integer x : arr) logger.info("-> " + x);
            drainBuffer(sourceIdx);
            logger.info("Total count:" + distributions[sourceIdx].totalCount());
            return distributions[sourceIdx].getTop(10);
        } else {
            // Find index
            String[] indexFiles = new File(workingDir).list(new FilenameFilter() {
                @Override
                public boolean accept(File file, String s) {
                    return (s.contains("drunkard_index."));
                }
            });

            // Ugly
            String curFile = null;
            int curIndex = 0;
            for(String indexFile : indexFiles) {
                logger.info("i: " + indexFile);
                int indexStart = Integer.parseInt(indexFile.substring(indexFile.lastIndexOf(".") + 1));
                if (indexStart <= vertexId) {

                    if (curFile == null || curIndex < indexStart) {
                        curIndex = indexStart;
                        curFile = indexFile;
                    }
                }
            }
            if (curFile == null) throw new RuntimeException("Vertex not found in database!");
            else {
                logger.info("Reading: " + curFile);
                try {
                    File ifile = new File(workingDir, curFile);
                    // TODO: document
                    byte[] fnamebytes = new byte[(int)ifile.length()];
                    new FileInputStream(ifile).read(fnamebytes);
                    String fileName = new String(fnamebytes);
                    logger.info("Data file:" + fileName);
                    RandomAccessFile raf = new RandomAccessFile(fileName, "r");
                    int rowLength = 4 + 8 * 10;
                    int idx = (vertexId - curIndex) * rowLength;
                    byte[] data = new byte[rowLength];
                    raf.seek(idx);
                    raf.read(data);

                    logger.info("Seek: " + idx);

                    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
                    int vid = dis.readInt();

                    IdCount[] result = new IdCount[10];
                    for(int j=0; j<result.length; j++) {
                        result[j] = new IdCount(dis.readInt(), dis.readInt());
                        logger.info(result[j] + "");
                    }
                    if (vid != vertexId) throw new RemoteException("Mismatch: " + vid + " != " + vertexId);

                    return result;
                } catch (Exception e) {
                    throw new RemoteException(e.getMessage());
                }

            }
        }
    }


    private void drainBuffer(int sourceIdx) {
        synchronized (buffers[sourceIdx]) {
            int[] arr = buffers[sourceIdx].toIntArray();
            buffers[sourceIdx] = new IntegerBuffer(BUFFER_CAPACITY);
            Arrays.sort(arr);
            DiscreteDistribution dist = new DiscreteDistribution(arr);
            mergeWith(sourceIdx, dist);
        }
    }

    @Override
    public void processWalks(final int[] walks, final int[] atVertices) throws RemoteException {
        try {
            pendingQueue.put(new WalkSubmission(walks, atVertices));
            int pending = pendingQueue.size();
            if (pending > 50 && pending % 20 == 0) {
                logger.info("Warning, pending queue size: " + pending);
            }
        } catch (Exception err) {
            err.printStackTrace();
        }
    }

    @Override
    public void outputDistributions(String outputFile) throws RemoteException {
        logger.info("Waiting for processing to finish");
        while(outstanding.get() > 0) {
            logger.info("...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("Write output...");
        try {
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(
                    new File(workingDir, outputFile))));

            for(int i=0; i<sourceVertexIds.length; i++) {
                int sourceVertex = sourceVertexIds[i];
                drainBuffer(i);
                DiscreteDistribution distr = distributions[i];
                IdCount[] topVertices = distr.getTop(10);
                dos.writeInt(sourceVertex);
                int written = 0;
                for(IdCount vc : topVertices) {
                    dos.writeInt(vc.id);
                    dos.writeInt(vc.count);
                    written++;
                }
                while(written < 10) {
                    written++;
                    dos.writeInt(-1);
                    dos.writeInt(-1);
                }
            }
            dos.close();

            // Write index
            File directory = new File(outputFile).getParentFile();
            File indexFile = new File(directory, "drunkard_index." + sourceVertexIds[0]);
            BufferedWriter wr = new BufferedWriter(new FileWriter(indexFile));
            wr.write(outputFile);
            wr.close();
            logger.info("Done...");

        } catch (Exception err) {
            err.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        Double pruneFraction = Double.parseDouble(args[0]);
        String workingDir = args[1];
        String bindAddress = args[2];
        try {
            LocateRegistry.createRegistry(1099);
        } catch (Exception err) {
            logger.info("Registry already created?");
        }
        Naming.rebind(bindAddress, new DrunkardCompanion(pruneFraction, workingDir));
        logger.info("Prune fraction: " + pruneFraction);
    }

}
