package edu.cmu.graphchi.aggregators;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.engine.auxdata.VertexData;

import java.io.*;
import java.util.Iterator;

/**
 * Maps vertex values to new values
 */
public class VertexMapper {

    public static <VertexDataType> void map(int numVertices, String baseFilename, BytesToValueConverter<VertexDataType> conv,
                                            VertexMapperCallback<VertexDataType> callback) throws IOException {

        VertexData<VertexDataType> vertexData = new VertexData<VertexDataType>(numVertices, baseFilename, conv, true);

        DataBlockManager blockManager = new DataBlockManager();
        vertexData.setBlockManager(blockManager);

        int CHUNK = 1000000;
        for(int i=0; i < numVertices; i += CHUNK) {
            int en = i + CHUNK;
            if (en >= numVertices) en = numVertices - 1;
            int blockId =  vertexData.load(i, en);

            Iterator<Integer> iter = vertexData.currentIterator();

            while (iter.hasNext()) {
                int j = iter.next();
                ChiPointer ptr = vertexData.getVertexValuePtr(j, blockId);
                VertexDataType oldValue = blockManager.dereference(ptr, conv);
                VertexDataType newValue = callback.map(j, oldValue);
                blockManager.writeValue(ptr, conv, newValue);
            }
            vertexData.releaseAndCommit(i, blockId);
        }

    }
}
