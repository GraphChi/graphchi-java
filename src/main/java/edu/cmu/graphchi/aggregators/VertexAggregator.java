package edu.cmu.graphchi.aggregators;

/**
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.auxdata.VertexData;

import java.io.*;


/**
 * Compute aggregates over the vertex values.
 */
public class VertexAggregator {


    public static <VertexDataType> void  foreach(int numVertices, String baseFilename, BytesToValueConverter<VertexDataType> conv,
                                                 ForeachCallback<VertexDataType> callback) throws IOException {

        VertexData<VertexDataType> vertexData = new VertexData<VertexDataType>(numVertices, baseFilename, conv, true);

        DataBlockManager blockManager = new DataBlockManager();
        vertexData.setBlockManager(blockManager);

        int CHUNK = 1000000;
        for(int i=0; i < numVertices; i += CHUNK) {
            int en = i + CHUNK;
            if (en >= numVertices) en = numVertices - 1;
            int blockId =  vertexData.load(i, en);

            for(int j=i; j<en; j++) {
                ChiPointer ptr = vertexData.getVertexValuePtr(j, blockId);
                VertexDataType value = blockManager.dereference(ptr, conv);
                callback.callback(j, value);
            }
        }

    }

    public static <VertexDataType> void  foreach( String baseFilename, BytesToValueConverter<VertexDataType> conv,
                                                 ForeachCallback<VertexDataType> callback) throws IOException {

        try {
            VertexAggregator.foreach(Integer.MAX_VALUE, baseFilename, conv, callback);
        } catch (EOFException eo) {} // Ugly!

    }

    private static class SumCallbackInt implements ForeachCallback<Integer> {
        long sum = 0;
        @Override
        public void callback(int vertexId, Integer vertexValue) {
            sum += vertexValue;
        }

        public long getSum() {
            return sum;
        }
    }

    public static long sumInt(int numVertices, String baseFilename) throws IOException {
        SumCallbackInt callback = new SumCallbackInt();
        foreach(numVertices, baseFilename, new IntConverter(), callback);
        return callback.getSum();
    }

}
