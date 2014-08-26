package edu.cmu.graphchi.walks.distributions;

import edu.cmu.graphchi.walks.LongWalkManager;
import edu.cmu.graphchi.walks.WalkArray;
import edu.cmu.graphchi.walks.LongWalkArray;

import java.rmi.RemoteException;

public class LongDrunkardCompanion extends DrunkardCompanion {
    private LongWalkManager manager;

    public LongDrunkardCompanion( final int numThreads, final long maxMemoryBytes)
            throws RemoteException {
        super(numThreads, maxMemoryBytes);
        // TODO: may be better to pass this in...
        manager = new LongWalkManager(0, 0);
    }

    @Override
    protected void _processWalks(WalkArray walkArray, int[] atVertices) {
        long[] walks = ((LongWalkArray)walkArray).getArray();
        long t1 = System.currentTimeMillis();
        for(int i=0; i < walks.length; i++) {
            long w = walks[i];
            int atVertex = atVertices[i];
            int sourceIdx = manager.sourceIdx(w);

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
}
