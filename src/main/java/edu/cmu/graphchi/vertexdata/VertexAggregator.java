package edu.cmu.graphchi.vertexdata;

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


import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.auxdata.VertexData;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

import java.io.*;
import java.util.Iterator;


/**
 * Efficient iteration over vertex values for computing aggregates.
 * @author Aapo Kyrola
 */
public class VertexAggregator {


    /**
     * Enumerate vertices using a callback
     * @see edu.cmu.graphchi.vertexdata.ForeachCallback
     * @param numVertices number of vertices in the graph (hint: use engine.numVertices())
     * @param baseFilename name of the input graph
     * @param conv converter object for converting bytes to vertex's value type
     * @param callback your callback function
     * @param <VertexDataType> vertex data type
     * @throws IOException if the vertex data file is not found
     */
    public static <VertexDataType> void foreach(int numVertices, String baseFilename, BytesToValueConverter<VertexDataType> conv,
                                                 ForeachCallback<VertexDataType> callback) throws IOException {

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
                VertexDataType value = blockManager.dereference(ptr, conv);
                callback.callback(j, value);
            }
        }
    }

    /**
     * Returns an iterator to vertices. Vertices are iterated in their internal-order,
     * but the iterator elements have the original ids.
     * @param numVertices number of vertices in the graph (hint: use engine.numVertices())
     * @param baseFilename name of the input graph
     * @param conv converter object for converting bytes to vertex's value type
     * @param idTranslate translates ids from internal id to original id (use engine.getVertexIdTranslate())
     * @param <VertexDataType>
     * @return
     */
    public static <VertexDataType> Iterator<VertexIdValue<VertexDataType> > vertexIterator(final int numVertices,
                                                                                            String baseFilename,
                                                                                            final BytesToValueConverter<VertexDataType> conv,
                                                                                            final VertexIdTranslate idTranslate) throws IOException {
        final VertexData<VertexDataType> vertexData = new VertexData<VertexDataType>(numVertices, baseFilename, conv, true);

        final DataBlockManager blockManager = new DataBlockManager();
        vertexData.setBlockManager(blockManager);

        final int CHUNK = 1000000;

        return new Iterator<VertexIdValue<VertexDataType>>() {

            int i=0, blockId;
            Iterator<Integer> curIter;

            @Override
            public boolean hasNext() {
                if (i >= numVertices - 1) return false;
                if (curIter == null || !curIter.hasNext()) {
                    int en = i + CHUNK;
                    if (en >= numVertices) en = numVertices - 1;

                    try {
                        blockId =  vertexData.load(i, en);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    this.curIter = vertexData.currentIterator();
                }
                return true;
            }

            @Override
            public VertexIdValue next() {
                if (hasNext()) {
                    i = curIter.next();
                    ChiPointer ptr = vertexData.getVertexValuePtr(i, blockId);
                    return new VertexIdValue<VertexDataType>(idTranslate.backward(i), blockManager.dereference(ptr, conv));
                } else throw new IllegalStateException("No more elements in the iterator!");
            }

            @Override
            public void remove() {
                throw new RuntimeException("Remove() not implemented");
            }
        };
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

    /**
     * Compute a sum of vertex-values (assumed to be integers)
     * @param numVertices number of vertices in the graph (hint: use engine.numVertices())
     * @param baseFilename name of the input graph
     * @return the sum
     * @throws IOException if the vertex data file is not found
     */
    public static long sumInt(int numVertices, String baseFilename) throws IOException {
        SumCallbackInt callback = new SumCallbackInt();
        foreach(numVertices, baseFilename, new IntConverter(), callback);
        return callback.getSum();
    }

}
