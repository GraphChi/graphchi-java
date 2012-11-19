package com.twitter.pers.graphchi.walks.distributions;

import com.twitter.pers.graphchi.walks.WalkManager;
import edu.cmu.graphchi.util.IdCount;
import edu.cmu.graphchi.util.IntegerBuffer;

import java.io.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DrunkardCompanion is a remote service that receives walks from the walker
 * and maintains a distribution from each source.
 */
public class DrunkardCompanion extends UnicastRemoteObject implements RemoteDrunkardCompanion {

    private static final int BUFFER_CAPACITY = 64;
    private static final int BUFFER_MAX = 512;

    private int maxOutstanding = 4096;

    private int[] sourceVertexIds;
    private Object[] distrLocks;

    private DiscreteDistribution[] distributions;
    private IntegerBuffer[] buffers;
    private AtomicInteger outstanding = new AtomicInteger(0);

    private ExecutorService parallelExecutor;
    private Double pruneFraction;


    public DrunkardCompanion(double pruneFraction) throws RemoteException {
        parallelExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.pruneFraction = pruneFraction;
    }

    private void mergeWith(int sourceIdx, DiscreteDistribution distr) {
        synchronized (distrLocks[sourceIdx]) {
            distributions[sourceIdx] = DiscreteDistribution.merge(distributions[sourceIdx], distr);

            int sz = distributions[sourceIdx].size();
            if (sz > 500) {
                int mx = distributions[sourceIdx].max();
                int pruneLimit = (int) (mx * pruneFraction);
                if (pruneLimit > 0) {
                    DiscreteDistribution filtered =  distributions[sourceIdx].filteredAndShift(pruneLimit);
                    if (filtered.size() > 25) {
                        distributions[sourceIdx] = filtered;
                        int prunedSize = distributions[sourceIdx].size();
                        //  System.out.println("Pruned: " + sz + " => " + prunedSize + " max: " + mx + ", limit=" + pruneLimit);
                    } else {
                        //  System.out.println("Filtering would have deleted almost everything...");
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
        System.out.println("Initializing sources...");
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
        System.out.println("Done...");
    }


    private void _processWalks(int[] walks, int[] atVertices) {
        long t1 = System.currentTimeMillis();
        for(int i=0; i < walks.length; i++) {
            int w = walks[i];
            int atVertex = atVertices[i];
            int sourceIdx = WalkManager.sourceIdx(w);

            if (atVertex == sourceVertexIds[sourceIdx]) {
                // Ignore - at origin
                continue;
            }

            IntegerBuffer drainArr = null;
            buffers[sourceIdx].add(atVertex);
            if (buffers[sourceIdx].size() >= BUFFER_MAX) {
                drainArr = buffers[sourceIdx];
                buffers[sourceIdx] = new IntegerBuffer(BUFFER_CAPACITY);
            }

            // Do hard part of the draining outside of lock
            if (drainArr != null) {
                outstanding.incrementAndGet();
                final IntegerBuffer toDrain = drainArr;
                final int drainIdx = sourceIdx;

                parallelExecutor.submit(new Runnable() { public void run() {
                    int[] d = toDrain.toIntArray();
                    Arrays.sort(d);
                    DiscreteDistribution dist = new DiscreteDistribution(d);
                    mergeWith(drainIdx, dist);
                    outstanding.decrementAndGet();
                }});
            }
        }
        System.out.println("Processing " + walks.length + " took " + (System.currentTimeMillis() - t1) + " ms.");

    }

    private void drainBuffer(int sourceIdx) {
        int[] arr = buffers[sourceIdx].toIntArray();
        buffers[sourceIdx] = new IntegerBuffer(BUFFER_CAPACITY);
        Arrays.sort(arr);
        DiscreteDistribution dist = new DiscreteDistribution(arr);
        mergeWith(sourceIdx, dist);
    }

    @Override
    public void processWalks(final int[] walks, final int[] atVertices) throws RemoteException {
        while(outstanding.get() > maxOutstanding) {
            System.out.println("Flow control...");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        _processWalks(walks, atVertices);
    }

    @Override
    public void outputDistributions(String outputFile) throws RemoteException {
        System.out.println("Waiting for processing to finish");
        while(outstanding.get() > 0) {
            System.out.println("...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Write output...");
        try {
            BufferedWriter wr = new BufferedWriter(new FileWriter(outputFile));

            for(int i=0; i<sourceVertexIds.length; i++) {
                int sourceVertex = sourceVertexIds[i];
                drainBuffer(i);
                DiscreteDistribution distr = distributions[i];
                TreeSet<IdCount> topVertices = distr.getTop(10);
                wr.write(sourceVertex + "\t");
                for(IdCount vc : topVertices) {
                    wr.write("\t");
                    wr.write(vc.id + "," + vc.count);
                }
                wr.write("\n");
            }
            wr.close();

        } catch (Exception err) {
            err.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Double pruneFraction = Double.parseDouble(args[0]);
        LocateRegistry.createRegistry(1099);
        Naming.rebind("drunkarcompanion", new DrunkardCompanion(pruneFraction));
        System.out.println("Bound to " + Naming.list("dru*")[0]);
        System.out.println("Prune fraction: " + pruneFraction);
    }

}
