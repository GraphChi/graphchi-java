package edu.cmu.graphchi;

import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.engine.auxdata.VertexDegree;

import java.util.concurrent.atomic.AtomicInteger;

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
public class ChiVertex<VertexValue, EdgeValue> {

    /**
     *  To save memory, support now only 32-bit vertex ids.
     */
    private int id;
    public static DataBlockManager blockManager;
    public static BytesToValueConverter vertexValueConverter;
    public static BytesToValueConverter edgeValueConverter;

    private int nInedges = 0;
    private int[] inEdgeDataChunks = null;
    private int[] inEdgeDataOffsets = null;
    private int[] inEdgeVertexIds = null;

    private AtomicInteger nOutedges = new AtomicInteger(0);
    private int[] outEdgeDataChunks = null;
    private int[] outEdgeDataOffsets = null;
    private int[] outEdgeVertexIds = null;

    /* Internal management */
    public boolean scheduled = true;
    public boolean parallelSafe = true;

    private ChiPointer vertexPtr;

    public ChiVertex(int id, VertexDegree degree) {
        this.id = id;
        inEdgeDataChunks = new int[degree.inDegree];
        inEdgeDataOffsets = new int[degree.inDegree];
        inEdgeVertexIds = new int[degree.inDegree];
        outEdgeDataChunks = new int[degree.outDegree];
        outEdgeDataOffsets = new int[degree.outDegree];
        outEdgeVertexIds = new int[degree.outDegree];
    }


    public long getId() {
        return this.id;
    }

    public void setDataPtr(ChiPointer vertexPtr) {
        this.vertexPtr = vertexPtr;
    }


    public VertexValue getValue() {
        return blockManager.dereference(vertexPtr, (BytesToValueConverter<VertexValue>)
                vertexValueConverter);
    }

    public void setValue(VertexValue x) {
        blockManager.writeValue(vertexPtr, vertexValueConverter, x);
    }


    public int numOutEdges() {
        return nOutedges.get();
    }

    public int numInEdges() {
        return nInedges;
    }

    public void addInEdge(int chunkId, int offset, int vertexId) {
        inEdgeDataChunks[nInedges] = chunkId;
        inEdgeDataOffsets[nInedges] = offset;
        inEdgeVertexIds[nInedges] = vertexId;
        nInedges++;

    }




    public void addOutEdge(int chunkId, int offset,  int vertexId) {
        int tmpOutEdges = nOutedges.addAndGet(1) - 1;
        outEdgeDataChunks[tmpOutEdges] = chunkId;
        outEdgeDataOffsets[tmpOutEdges] = offset;
        outEdgeVertexIds[tmpOutEdges] = vertexId;

    }

    public ChiEdge<EdgeValue> inEdge(int i) {
        return new Edge(new ChiPointer(inEdgeDataChunks[i], inEdgeDataOffsets[i]), inEdgeVertexIds[i]);
    }

    public ChiEdge<EdgeValue>  outEdge(int i) {
        return new Edge(new ChiPointer(outEdgeDataChunks[i], outEdgeDataOffsets[i]), outEdgeVertexIds[i]);
    }

    public ChiEdge<EdgeValue> edge(int i) {
        if (i < nInedges) return inEdge(i);
        else return outEdge(i - nInedges);
    }



    class Edge implements ChiEdge<EdgeValue> {
        Edge(ChiPointer dataPtr, int vertexId) {
            this.dataPtr = dataPtr;
            this.vertexId = vertexId;
        }

        ChiPointer dataPtr;
        int vertexId;

        public int getVertexId() {
            return  vertexId;
        }

        public EdgeValue getValue() {
            return blockManager.dereference(dataPtr, (BytesToValueConverter<EdgeValue>) edgeValueConverter);
        }

        public void setValue(EdgeValue x) {
            blockManager.writeValue(dataPtr, edgeValueConverter, x);
        }
    }

}
