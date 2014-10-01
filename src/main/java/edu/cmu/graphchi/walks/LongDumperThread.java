package edu.cmu.graphchi.walks;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LongDumperThread extends DumperThread {
    protected final DrunkardJob job;
    protected long[] walks = new long[256 * 1024];
    protected int[] vertices = new int[256 * 1024];
    protected int idx = 0;

    public LongDumperThread(LinkedBlockingQueue<BucketsToSend> bucketQueue,
                            AtomicLong pendingWalksToSubmit,
                            AtomicBoolean finished,
                            DrunkardJob job) {
      super(bucketQueue, pendingWalksToSubmit, finished);
      this.job = job;
    }

    @Override
    protected void processWalks(BucketsToSend bucket, int i) {
        LongWalkArray bucketWalks = (LongWalkArray) bucket.walks;
        long w = bucketWalks.getArray()[i];
        LongWalkManager manager = (LongWalkManager) job.getWalkManager();
        int v = manager.off(w) + bucket.firstVertex;


        // Skip walks with the track-bit (hop-bit) not set
        boolean trackBit = manager.trackBit(w);

        if (!trackBit) {
            return;
        }

        walks[idx] = w;
        vertices[idx] = v;
        idx++;

        if (idx >= walks.length) {
            try {
                job.getCompanion().processWalks(new LongWalkArray(walks), vertices);
            } catch (Exception err) {
                err.printStackTrace();
            }
            idx = 0;
        }
    }

    @Override
    protected void sendRest() {
        // Send rest
        try {
            long[] tmpWalks = new long[idx];
            int[] tmpVertices = new int[idx];
            System.arraycopy(walks, 0, tmpWalks, 0, idx);
            System.arraycopy(vertices, 0, tmpVertices, 0, idx);
            job.getCompanion().processWalks(new LongWalkArray(tmpWalks), tmpVertices);
        } catch (Exception err) {
            err.printStackTrace();
        }
    }
}

