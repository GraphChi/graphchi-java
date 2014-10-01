package edu.cmu.graphchi.walks;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class DumperThread implements Runnable {

    private final LinkedBlockingQueue<BucketsToSend> bucketQueue;
    private final AtomicLong pendingWalksToSubmit;
    private final AtomicBoolean finished;

    public DumperThread(LinkedBlockingQueue<BucketsToSend> bucketQueue,
                        AtomicLong pendingWalksToSubmit,
                        AtomicBoolean finished) {
      this.bucketQueue = bucketQueue;
      this.pendingWalksToSubmit = pendingWalksToSubmit;
      this.finished = finished;
    }

    public void run() {
        while(!finished.get() || bucketQueue.size() > 0) {
            BucketsToSend bucket = null;
            try {
                bucket = bucketQueue.poll(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }
            if (bucket != null) {
                pendingWalksToSubmit.addAndGet(-bucket.length);
                for(int i=0; i<bucket.length; i++) {
                    processWalks(bucket, i);
                }
            }
        }
        sendRest();
    }

    protected abstract void processWalks(BucketsToSend bucket, int i);

    protected abstract void sendRest();
}

