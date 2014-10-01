package edu.cmu.graphchi.walks;

public class LongDrunkardFactory<VertexDataType, EdgeDataType>
        implements DrunkardFactory<VertexDataType, EdgeDataType> {
    public DrunkardDriver<VertexDataType, EdgeDataType> createDrunkardDriver(DrunkardJob job,
            WalkUpdateFunction<VertexDataType, EdgeDataType> callback) {
        return new LongDrunkardDriver(job, callback);
    }
    public WalkManager createWalkManager(int numVertices, int numSources) {
        return new LongWalkManager(numVertices, numSources);
    }
    public LocalWalkBuffer createLocalWalkBuffer() {
        return new LongLocalWalkBuffer();
    }
}

