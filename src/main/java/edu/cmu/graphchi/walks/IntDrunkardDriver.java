package edu.cmu.graphchi.walks;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

/**
 * Class to encapsulate the graphchi program running the show.
 * Due to several optimizations, it is quite complicated!
 */
public class IntDrunkardDriver<VertexDataType, EdgeDataType>
        extends DrunkardDriver<VertexDataType, EdgeDataType> implements GrabbedBucketConsumer {

    IntDrunkardDriver(final DrunkardJob job,
            WalkUpdateFunction<VertexDataType, EdgeDataType> callback) {
        super(job, callback);
    }

    @Override
    protected IntDumperThread createDumperThread() {
        return new IntDumperThread();
    }

    protected class IntDumperThread extends DrunkardDriver.DumperThread {
        private int[] walks = new int[256 * 1024];
        private int[] vertices = new int[256 * 1024];
        private int idx = 0;

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

    @Override
    protected DrunkardContext createDrunkardContext(int vertexId, final GraphChiContext context,
            final LocalWalkBuffer localBuf_) {
        final IntWalkManager manager = (IntWalkManager) job.getWalkManager();
        final boolean  isSource = manager.isSource(vertexId);
        final int mySourceIndex = (isSource ? manager.getVertexSourceIdx(vertexId) : -1);
        final IntLocalWalkBuffer localBuf = (IntLocalWalkBuffer) localBuf_;
        return new IntDrunkardContext() {
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
            public void forwardWalkTo(int walk, int destinationVertex, boolean trackBit) {
                localBuf.add(walk, destinationVertex, trackBit);
            }

            @Override
            public void resetWalk(int walk, boolean trackBit) {
                forwardWalkTo(walk, manager.getSourceVertex(walk), trackBit);
            }

            @Override
            public boolean getTrackBit(int walk) {
                return manager.trackBit(walk);
            }

            @Override
            public boolean isWalkStartedFromVertex(int walk) {
                return mySourceIndex == manager.sourceIdx(walk);
            }

            @Override
            public VertexIdTranslate getVertexIdTranslate() {
                return getVertexIdTranslate();
            }
        };
    }
}

