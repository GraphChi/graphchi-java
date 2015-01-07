package edu.cmu.graphchi.walks.distributions;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.walks.WalkArray;
import edu.cmu.graphchi.walks.LongWalkArray;
import edu.cmu.graphchi.walks.distributions.DiscreteDistribution;
import edu.cmu.graphchi.walks.distributions.RemoteDrunkardCompanion;
import edu.cmu.graphchi.util.IdCount;
import edu.cmu.graphchi.util.IntegerBuffer;

import java.io.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;


/**
 * A DrunkardCompanion object that has two keys to get to a DiscreteDistribution, instead of one.
 * Where DrunkardCompanion represents a matrix of values (one key to get to a DiscreteDistribution
 * vector), this represents a rank 3 tensor (two keys to get to a DiscreteDistribution).  This is
 * suitable for collecting more complicated statistics than DrunkardCompanion, though the current
 * implementation is perhaps a little slower than it could be, using nested hash maps instead of a
 * more efficient data structure.
 */
public abstract class TwoKeyCompanion extends UnicastRemoteObject
        implements RemoteDrunkardCompanion {

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

    boolean isLowInMemory = false;

    // Using hash maps of hash maps isn't the most efficient thing to do here, but it'll do for
    // now.
    protected ConcurrentHashMap<Integer,
              ConcurrentHashMap<Integer, DiscreteDistribution>> distributions;
    protected ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, IntegerBuffer>> buffers;
    protected ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Object>> distrLocks;
    protected AtomicInteger outstanding = new AtomicInteger(0);

    protected ExecutorService parallelExecutor;
    protected long maxMemoryBytes;

    protected LinkedBlockingQueue<WalkSubmission> pendingQueue = new LinkedBlockingQueue<WalkSubmission>();

    protected static Logger logger = ChiLogger.getLogger("pathcompanion");
    protected Timer timer  = new Timer(true);

    /**
     * Prints estimate of memory usage
     */
    private long memoryAuditReport() {
        long companionOverHeads = 0;

        long bufferMem = 0;
        long maxMem = 0;
        int bufferCount = 0;
        for (ConcurrentHashMap<Integer, IntegerBuffer> map : buffers.values()) {
            companionOverHeads += 4;
            for(IntegerBuffer buf : map.values()) {
                bufferCount += 1;
                companionOverHeads += 4;
                long est = buf.memorySizeEst();
                bufferMem += est;
                maxMem = Math.max(maxMem, est);
            }
        }

        long distributionMem = 0;
        long maxDistMem = 0;
        long avoidMem = 0;
        int distCount = 0;
        for (ConcurrentHashMap<Integer, DiscreteDistribution> map : distributions.values()) {
            companionOverHeads += 4;
            for(DiscreteDistribution dist : map.values()) {
                distCount += 1;
                companionOverHeads += 4;
                long est = dist.memorySizeEst();
                distributionMem += est;
                maxDistMem = Math.max(est, maxDistMem);
                avoidMem += dist.avoidCount() * 6;
            }
        }

        NumberFormat nf = NumberFormat.getInstance(Locale.US);

        logger.info("======= MEMORY REPORT ======");
        logger.info("Companion internal: " + nf.format(companionOverHeads / 1024. / 1024.) + " mb");

        logger.info("Buffer mem: " + nf.format(bufferMem / 1024. / 1024.) + " mb");
        logger.info("Avg bytes per buffer: " +
                nf.format(bufferMem * 1.0 / bufferCount / 1024.) + " kb");
        logger.info("Max buffer was: " + nf.format(maxMem / 1024.) + "kb");

        logger.info("Distribution mem: " + nf.format(distributionMem / 1024. / 1024.) + " mb");
        logger.info("- of which avoids: " + nf.format(avoidMem / 1024. / 1024.) + " mb");

        logger.info("Avg bytes per distribution: " +
                nf.format((distributionMem * 1.0 / distCount / 1024.)) + " kb");
        logger.info("Max distribution: " + nf.format(maxDistMem / 1024.) + " kb");

        long totalMem = companionOverHeads + bufferMem + distributionMem;
        logger.info("** Total:  " + nf.format(totalMem / 1024. / 1024. / 1024.) +
                " GB (low-mem limit " +
                Runtime.getRuntime().maxMemory() * 0.75 / 1024. / 1024. / 1024. + "GB)" );
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

        for (Integer firstKey : distributions.keySet()) {
            ConcurrentHashMap<Integer, DiscreteDistribution> map = distributions.get(firstKey);
            for (Integer secondKey : map.keySet()) {
                DiscreteDistribution prevDist, newDist;
                synchronized (distrLocks.get(firstKey).get(secondKey)) {
                    prevDist = map.get(secondKey);
                    newDist =  prevDist.filteredAndShift(2);
                    map.put(secondKey, newDist);
                }
                before += prevDist.memorySizeEst();
                after += newDist.memorySizeEst();
            }
        }

        logger.info("** Compacted: " + (before / 1024. / 1024. / 1024.) + " GB --> " +
                (after / 1024. / 1024. / 1024.) + " GB");
    }


    /**
     * Creates the TwoKeyCompanion object
     * @param numThreads number of worker threads (4 is common)
     * @param maxMemoryBytes maximum amount of memory to use for storing the distributions
     */
    public TwoKeyCompanion(int numThreads, long maxMemoryBytes) throws RemoteException {
        this.maxMemoryBytes = maxMemoryBytes;
        parallelExecutor =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        buffers = new ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, IntegerBuffer>>();
        distrLocks = new ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Object>>();
        distributions = new ConcurrentHashMap<Integer,
                      ConcurrentHashMap<Integer, DiscreteDistribution>>();


        for(int threadId=0; threadId < numThreads; threadId++) {
            Thread processingThread = new Thread(new ProcessingThread(threadId, numThreads));
            processingThread.setDaemon(true);
            processingThread.start();
        }
    }

    private class ProcessingThread implements Runnable {
        private int id;
        private int numThreads;

        public ProcessingThread(int id, int numThreads) {
            this.id = id;
            this.numThreads = numThreads;
        }
        @Override
        public void run() {
            try {
                long unpurgedWalks = 0;
                while(true) {
                    WalkSubmission subm = pendingQueue.poll(2000, TimeUnit.MILLISECONDS);
                    if (subm != null) {
                        _processWalks(subm.walks, subm.atVertices);
                        unpurgedWalks += subm.walks.size();
                    }
                    if (distributions != null) {
                        if (unpurgedWalks > distributions.size() * 10 ||
                                (subm == null && unpurgedWalks > 100000)) {
                            logger.fine("Purge:" + unpurgedWalks);
                            unpurgedWalks = 0;

                            // Loop to see what to drain. Every thread looks for
                            // different buffers.
                            for (Integer firstKey : buffers.keySet()) {
                                ConcurrentHashMap<Integer, IntegerBuffer> map =
                                    buffers.get(firstKey);
                                for (Integer secondKey : map.keySet()) {
                                    if ((firstKey + secondKey) % numThreads != id) {
                                        continue;
                                    }
                                    // Drain asynchronously
                                    outstanding.incrementAndGet();
                                    final IntegerBuffer toDrain = map.get(secondKey);
                                    final int first = firstKey;
                                    final int second = secondKey;

                                    synchronized (toDrain) {
                                        map.put(secondKey, new IntegerBuffer(BUFFER_CAPACITY));
                                    }
                                    parallelExecutor.submit(new Runnable() { public void run() {
                                        try {
                                            int[] d = toDrain.toIntArray();
                                            Arrays.sort(d);
                                            DiscreteDistribution dist = new DiscreteDistribution(d);
                                            mergeWith(first, second, dist);
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
                if (!(err instanceof InterruptedException)) {
                    err.printStackTrace();
                }
            }
        }
    }

    protected void ensureExists(int firstKey, int secondKey) {
        ConcurrentHashMap<Integer, Object> map = distrLocks.get(firstKey);
        if (map == null) {
            ConcurrentHashMap<Integer, Object> new_map = new ConcurrentHashMap<Integer, Object>();
            map = distrLocks.putIfAbsent(firstKey, new_map);
            if (map == null) {
                map = new_map;
            }
        }
        Object lock = map.get(secondKey);
        if (lock == null) {
            Object new_lock = new Object();
            lock = map.putIfAbsent(secondKey, new_lock);
            if (lock == null) {
                synchronized(new_lock) {
                    ConcurrentHashMap<Integer, DiscreteDistribution> dmap =
                        distributions.get(firstKey);
                    if (dmap == null) {
                        dmap = new ConcurrentHashMap<Integer, DiscreteDistribution>();
                        distributions.put(firstKey, dmap);
                    }
                    dmap.put(secondKey, new DiscreteDistribution());
                    ConcurrentHashMap<Integer, IntegerBuffer> bmap = buffers.get(firstKey);
                    if (bmap == null) {
                        bmap = new ConcurrentHashMap<Integer, IntegerBuffer>();
                        buffers.put(firstKey, bmap);
                    }
                    bmap.put(secondKey, new IntegerBuffer(BUFFER_CAPACITY));
                }
            } else {
                synchronized(lock) {
                    // We're just waiting for the other thread to release the lock, so that we can
                    // get the buffer without crashing later.  Another thread actually added it,
                    // but we have to wait for them.
                }
            }
        }
    }

    private void mergeWith(int firstKey, int secondKey, DiscreteDistribution distr) {
        ensureExists(firstKey, secondKey);
        synchronized (distrLocks.get(firstKey).get(secondKey)) {
            DiscreteDistribution mergeInto = distributions.get(firstKey).get(secondKey);
            DiscreteDistribution merged = DiscreteDistribution.merge(mergeInto, distr);
            distributions.get(firstKey).put(secondKey, merged);
        }
    }

    @Override
    public void setAvoidList(int sourceIdx, int[] avoidList) throws RemoteException {
        // We don't need this, so this is a no-op
    }

    @Override
    public IdCount[] getTop(int vertexId, int nTop) throws RemoteException {
        // Not really useful for us
        return null;
    }

    @Override
    public void setSources(int[] sources) throws RemoteException {
        // We don't use an array of source indices, so we just take the opportunity to initialize
        // our objects.

        // Restart timer
        timer.cancel();
        timer = new Timer(true);

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                memoryAuditReport();
            }
        }, 5000, 60000);
    }

    protected void _processWalks(WalkArray walkArray, int[] atVertices) {
        long[] walks = ((LongWalkArray)walkArray).getArray();
        long t1 = System.currentTimeMillis();
        for(int i=0; i < walks.length; i++) {
            long w = walks[i];
            if (ignoreWalk(w)) {
                continue;
            }
            int atVertex = atVertices[i];
            int firstKey = getFirstKey(w, atVertex);
            int secondKey = getSecondKey(w, atVertex);
            int value = getValue(w, atVertex);

            ensureExists(firstKey, secondKey);
            IntegerBuffer buffer = buffers.get(firstKey).get(secondKey);
            synchronized (buffer) {
                buffer.add(value);
            }
        }

        long tt = (System.currentTimeMillis() - t1);
        if (tt > 1000) {
            logger.info("Processing " + walks.length + " took " + tt + " ms.");
        }
    }

    protected boolean ignoreWalk(long walk) {
        if (walk == 0) {
            return true;
        }
        return false;
    }

    protected abstract int getFirstKey(long walk, int atVertex);

    protected abstract int getSecondKey(long walk, int atVertex);

    protected abstract int getValue(long walk, int atVertex);

    protected void drainBuffer(int firstKey, int secondKey) {
        IntegerBuffer buffer = buffers.get(firstKey).get(secondKey);
        int[] arr;
        synchronized (buffer) {
            arr = buffer.toIntArray();
            buffers.get(firstKey).put(secondKey, new IntegerBuffer(BUFFER_CAPACITY));
        }
        Arrays.sort(arr);
        DiscreteDistribution dist = new DiscreteDistribution(arr);
        mergeWith(firstKey, secondKey, dist);
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

    protected void waitForFinish() {
        logger.info("Waiting for processing to finish");
        while (pendingQueue.size() > 0) {
            logger.info("...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        while(outstanding.get() > 0) {
            logger.info("...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public abstract void outputDistributions(String outputFile) throws RemoteException;

    @Override
    public void outputDistributions(String outputFile, int nTop) throws RemoteException {
        outputDistributions(outputFile);
    }

    public void close() {
        parallelExecutor.shutdown();
        timer.cancel();
        clearMemory();
    }

    protected void clearMemory() {
        distributions.clear();
        buffers.clear();
        distrLocks.clear();
    }
}
