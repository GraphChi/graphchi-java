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
public class LongDrunkardDriver<VertexDataType, EdgeDataType>
        extends DrunkardDriver<VertexDataType, EdgeDataType> implements GrabbedBucketConsumer {

    public LongDrunkardDriver(final DrunkardJob job,
            WalkUpdateFunction<VertexDataType, EdgeDataType> callback) {
        super(job, callback);
    }

    @Override
    protected LongDumperThread createDumperThread() {
        return new LongDumperThread(bucketQueue, pendingWalksToSubmit, finished, job);
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

