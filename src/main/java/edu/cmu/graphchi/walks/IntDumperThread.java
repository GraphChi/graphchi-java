package edu.cmu.graphchi.walks;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IntDumperThread extends DumperThread {
    private final DrunkardJob job;
    private int[] walks = new int[256 * 1024];
    private int[] vertices = new int[256 * 1024];
    private int idx = 0;

    public IntDumperThread(LinkedBlockingQueue<BucketsToSend> bucketQueue,
                           AtomicLong pendingWalksToSubmit,
                           AtomicBoolean finished,
                           DrunkardJob job) {
      super(bucketQueue, pendingWalksToSubmit, finished);
      this.job = job;
    }

    @Override
    protected void processWalks(BucketsToSend bucket, int i) {
        IntWalkManager manager = (IntWalkManager) job.getWalkManager();
        IntWalkArray bucketWalks = (IntWalkArray) bucket.walks;
        int w = bucketWalks.getArray()[i];
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
                job.getCompanion().processWalks(new IntWalkArray(walks), vertices);
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
            int[] tmpWalks = new int[idx];
            int[] tmpVertices = new int[idx];
            System.arraycopy(walks, 0, tmpWalks, 0, idx);
            System.arraycopy(vertices, 0, tmpVertices, 0, idx);
            job.getCompanion().processWalks(new IntWalkArray(tmpWalks), tmpVertices);
        } catch (Exception err) {
            err.printStackTrace();
        }
    }
}
