package edu.cmu.graphchi.walks.distributions;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.walks.WalkArray;
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
 * DrunkardCompanion is a remote (or local) service that receives walks from the DrunkardEngine
 * and maintains a distribution of visits from each source.
 * Done partially during internship at Twitter, Fall 2012
 * @author Aapo Kyrola, akyrola@cs.cmu.edu
 */
public abstract class DrunkardCompanion extends UnicastRemoteObject implements RemoteDrunkardCompanion {

    protected static class WalkSubmission {
        WalkArray walks;
        int[] atVertices;

        private WalkSubmission(WalkArray walks, int[] atVertices) {
            this.walks = walks;
            this.atVertices = atVertices;
        }
    }

    protected static final int BUFFER_CAPACITY = 128;
    protected static final int BUFFER_MAX = 128;

    protected int[] sourceVertexIds;
    protected Object[] distrLocks;
    boolean isLowInMemory = false;

    protected DiscreteDistribution[] distributions;
    protected IntegerBuffer[] buffers;
    protected AtomicInteger outstanding = new AtomicInteger(0);

    protected ExecutorService parallelExecutor;
    protected long maxMemoryBytes;

    protected LinkedBlockingQueue<WalkSubmission> pendingQueue = new LinkedBlockingQueue<WalkSubmission>();

    protected static Logger logger = ChiLogger.getLogger("drunkardcompanion");
    protected Timer timer  = new Timer(true);

    private boolean closed = false;

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
        logger.info("** Total:  " + nf.format(totalMem / 1024. / 1024. / 1024.) + " GB (low-mem limit " + Runtime.getRuntime().maxMemory() * 0.25 / 1024. / 1024. / 1024. + "GB)" );
        isLowInMemory = totalMem > maxMemoryBytes;

        if (isLowInMemory) {
            compactMemoryUsage();
        }

        return totalMem;
    }

    /**
     * Removes tails from distributions to save memory
     */
    private void compactMemoryUsage() {
        long before=0;
        long after=0;

        for(int i=0; i < distributions.length; i++) {
            DiscreteDistribution prevDist, newDist;
            synchronized (distrLocks[i]) {

                prevDist = distributions[i];
                newDist =  prevDist.filteredAndShift(2);
                distributions[i] = newDist;
            }
            before += prevDist.memorySizeEst();
            after += newDist.memorySizeEst();
        }

        logger.info("** Compacted: " + (before / 1024. / 1024. / 1024.) + " GB --> " + (after / 1024. / 1024. / 1024.) + " GB");
    }


    /**
     * Creates the DrunkardCompanion object
     * @param numThreads number of worker threads (4 is common)
     * @param maxMemoryBytes maximum amount of memory to use for storing the distributions
     * @throws RemoteException
     */
    public DrunkardCompanion( final int numThreads, final long maxMemoryBytes) throws RemoteException {
        this.maxMemoryBytes = maxMemoryBytes;
        parallelExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());


        for(int threadId=0; threadId < numThreads; threadId++) {
            final int _threadId = threadId;
            Thread processingThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {

                        long unpurgedWalks = 0;
                        while(!closed) {

                            WalkSubmission subm = pendingQueue.poll(2000, TimeUnit.MILLISECONDS);
                            if (subm != null) {
                                _processWalks(subm.walks, subm.atVertices);
                                unpurgedWalks += subm.walks.size();
                            }
                            if (sourceVertexIds != null) {
                                if (unpurgedWalks > sourceVertexIds.length * 10 || (subm == null && unpurgedWalks > 100000)) {
                                    logger.fine("Purge:" + unpurgedWalks);
                                    unpurgedWalks = 0;

                                    // Loop to see what to drain. Every thread looks for
                                    // different buffers.
                                    for(int i=_threadId; i < sourceVertexIds.length; i+=numThreads) {
                                        if (buffers[i].size() >= BUFFER_MAX || closed) {
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
            processingThread.setDaemon(true);
            processingThread.start();
        }
    }

    private void mergeWith(int sourceIdx, DiscreteDistribution distr) {
        synchronized (distrLocks[sourceIdx]) {
            distributions[sourceIdx] = DiscreteDistribution.merge(distributions[sourceIdx], distr);

    /*        if (pruneFraction > 0.0 && isLowInMemory) {
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
            }               */
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

    protected abstract void _processWalks(WalkArray walkArray, int[] atVertices);

    @Override
    public IdCount[] getTop(int vertexId, int nTop) throws RemoteException {
        int sourceIdx = (sourceVertexIds == null ? -1 : Arrays.binarySearch(sourceVertexIds, vertexId));
        if (sourceIdx >= 0) {
            int[] arr = buffers[sourceIdx].toIntArray();
            drainBuffer(sourceIdx);
            return distributions[sourceIdx].getTop(nTop);
        } else {
           throw new IllegalArgumentException("Vertex not found from memory. ");
        }
    }


    protected void drainBuffer(int sourceIdx) {
        synchronized (buffers[sourceIdx]) {
            int[] arr = buffers[sourceIdx].toIntArray();
            buffers[sourceIdx] = new IntegerBuffer(BUFFER_CAPACITY);
            Arrays.sort(arr);
            DiscreteDistribution dist = new DiscreteDistribution(arr);
            mergeWith(sourceIdx, dist);
        }
    }

    @Override
    public void processWalks(final WalkArray walks, final int[] atVertices) throws RemoteException {
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

    public void outputDistributions(String outputFile) throws RemoteException {
        outputDistributions(outputFile, 10);
    }

    /*
      Writes the top visit counts to a binary file.
     */
    public void outputDistributions(String outputFile, int nTop) throws RemoteException {
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
                    new File(outputFile))));

            for(int i=0; i<sourceVertexIds.length; i++) {
                int sourceVertex = sourceVertexIds[i];
                drainBuffer(i);
                DiscreteDistribution distr = distributions[i];
                IdCount[] topVertices = distr.getTop(nTop);
                dos.writeInt(sourceVertex);
                int written = 0;
                for(IdCount vc : topVertices) {
                    dos.writeInt(vc.id);
                    dos.writeInt(vc.count);
                    written++;
                }
                while(written < nTop) {
                    written++;
                    dos.writeInt(-1);
                    dos.writeInt(-1);
                }
            }
            dos.close();

        } catch (Exception err) {
            err.printStackTrace();
        }
    }

    public void close() {
        closed = true;
        timer.cancel();
    }

    public static void main(String[] args) throws Exception {
        Double pruneFraction = Double.parseDouble(args[0]);
        String bindAddress = args[1];
        try {
            LocateRegistry.createRegistry(1099);
        } catch (Exception err) {
            logger.info("Registry already created?");
        }
        // TODO? Not sure what the main class is used for; just for testing?  This may need to be
        // put into the subclass.
        Naming.rebind(bindAddress, new IntDrunkardCompanion(4, (long) (Runtime.getRuntime().maxMemory() * 0.75)));
        logger.info("Prune fraction: " + pruneFraction);
    }

}
