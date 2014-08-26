package edu.cmu.graphchi.walks;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

/**
 * Class to encapsulate the graphchi program running the show.
 * Due to several optimizations, it is quite complicated!
 */
public class LongDrunkardDriver<VertexDataType, EdgeDataType>
        extends DrunkardDriver<VertexDataType, EdgeDataType> implements GrabbedBucketConsumer {

    public LongDrunkardDriver(final DrunkardJob job,
            WalkUpdateFunction<VertexDataType, EdgeDataType> callback) {
        super(job, callback);
    }

    @Override
    protected LongDumperThread createDumperThread() {
        return new LongDumperThread();
    }

    protected class LongDumperThread extends DrunkardDriver.DumperThread {
        protected long[] walks = new long[256 * 1024];
        protected int[] vertices = new int[256 * 1024];
        protected int idx = 0;

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

    @Override
    protected DrunkardContext createDrunkardContext(int vertexId, final GraphChiContext context,
            final LocalWalkBuffer localBuf_) {
        final LongWalkManager manager = (LongWalkManager) job.getWalkManager();
        final boolean  isSource = manager.isSource(vertexId);
        final int mySourceIndex = (isSource ? manager.getVertexSourceIdx(vertexId) : -1);
        final LongLocalWalkBuffer localBuf = (LongLocalWalkBuffer) localBuf_;
        return new LongDrunkardContext() {
            @Override
            public boolean isSource() {
                return isSource;
            }

            @Override
            public int sourceIndex() {
                return mySourceIndex;
            }

            @Override
            public int getIteration() {
                return context.getIteration();
            }

            @Override
            public void forwardWalkTo(long walk, int destinationVertex, boolean trackBit) {
                localBuf.add(walk, destinationVertex, trackBit);
            }

            @Override
            public void resetWalk(long walk, boolean trackBit) {
                forwardWalkTo(walk, manager.getSourceVertex(walk), trackBit);
            }

            @Override
            public boolean getTrackBit(long walk) {
                return manager.trackBit(walk);
            }

            @Override
            public boolean isWalkStartedFromVertex(long walk) {
                return mySourceIndex == manager.sourceIdx(walk);
            }

            @Override
            public VertexIdTranslate getVertexIdTranslate() {
                return getVertexIdTranslate();
            }
        };
    }
}

