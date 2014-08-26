package edu.cmu.graphchi.walks;

public interface DrunkardFactory<VertexDataType, EdgeDataType> {
    public DrunkardDriver<VertexDataType, EdgeDataType> createDrunkardDriver(DrunkardJob job,
            WalkUpdateFunction<VertexDataType, EdgeDataType> callback);
    public WalkManager createWalkManager(int numVertices, int numSources);
    public LocalWalkBuffer createLocalWalkBuffer();
}
