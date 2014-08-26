package edu.cmu.graphchi.walks;

public interface GrabbedBucketConsumer {
     void consume(int firstVertexInBucket, WalkArray walkBucket, int len);
}
