package edu.cmu.graphchi.walks;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import edu.cmu.graphchi.GraphChiContext;
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
        return new IntDumperThread(bucketQueue, pendingWalksToSubmit, finished, job);
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

