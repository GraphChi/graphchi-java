package edu.cmu.graphchi.walks;

public class IntDrunkardFactory<VertexDataType, EdgeDataType>
        implements DrunkardFactory<VertexDataType, EdgeDataType> {
    public DrunkardDriver<VertexDataType, EdgeDataType> createDrunkardDriver(DrunkardJob job,
            WalkUpdateFunction<VertexDataType, EdgeDataType> callback) {
        return new IntDrunkardDriver(job, callback);
    }
    public WalkManager createWalkManager(int numVertices, int numSources) {
        return new IntWalkManager(numVertices, numSources);
    }
    public LocalWalkBuffer createLocalWalkBuffer() {
        return new IntLocalWalkBuffer();
    }
}

