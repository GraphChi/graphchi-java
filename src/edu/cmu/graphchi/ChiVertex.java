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
    private int[] inEdgeDataArray = null;

    private AtomicInteger nOutedges = new AtomicInteger(0);
    private int[] outEdgeDataArray = null;

    /* Internal management */
    public boolean parallelSafe = true;

    private ChiPointer vertexPtr;

    public ChiVertex(int id, VertexDegree degree) {
        this.id = id;
        inEdgeDataArray = new int[degree.inDegree * 3];
        outEdgeDataArray = new int[degree.outDegree * 3];
    }


    public int getId() {
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
        int idx = nInedges * 3;
        inEdgeDataArray[idx] = chunkId;
        inEdgeDataArray[idx + 1] = offset;
        inEdgeDataArray[idx + 2] = vertexId;
        nInedges++;

    }




    public void addOutEdge(int chunkId, int offset,  int vertexId) {
        int tmpOutEdges = nOutedges.addAndGet(1) - 1;
        int idx = tmpOutEdges * 3;
        outEdgeDataArray[idx] = chunkId;
        outEdgeDataArray[idx + 1] = offset;
        outEdgeDataArray[idx + 2] = vertexId;

    }

    public ChiEdge<EdgeValue> inEdge(int i) {
        int idx = i * 3;
        return new Edge(new ChiPointer(inEdgeDataArray[idx], inEdgeDataArray[idx + 1]), inEdgeDataArray[idx + 2]);
    }

    public ChiEdge<EdgeValue>  outEdge(int i) {
        int idx = i * 3;
        return new Edge(new ChiPointer(outEdgeDataArray[idx], outEdgeDataArray[idx + 1]), outEdgeDataArray[idx + 2]);
    }

    public ChiEdge<EdgeValue> edge(int i) {
        if (i < nInedges) return inEdge(i);
        else return outEdge(i - nInedges);
    }

    public int numEdges() {
        return nInedges + nOutedges.get();
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
