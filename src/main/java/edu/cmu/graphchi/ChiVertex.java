package edu.cmu.graphchi;

import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.engine.auxdata.VertexDegree;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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

/**
 * Represents a vertex. Vertex contains a value and a set of in- and out-edges.
 * @param <VertexValue>
 * @param <EdgeValue>
 */
public class ChiVertex<VertexValue, EdgeValue> {

    /**
     *  To save memory, support now only 32-bit vertex ids.
     */
    private int id;
    public static DataBlockManager blockManager;
    public static BytesToValueConverter vertexValueConverter;
    public static BytesToValueConverter edgeValueConverter;
    public static boolean disableInedges = false;
    public static boolean disableOutedges = false;

    private int nInedges = 0;
    private int[] inEdgeDataArray = null;

    private AtomicInteger nOutedges = new AtomicInteger(0);
    private int[] outEdgeDataArray = null;

    /* Internal management */
    public boolean parallelSafe = true;

    private ChiPointer vertexPtr;

    public ChiVertex(int id, VertexDegree degree) {
        this.id = id;

        if (degree != null) {
            if (!disableInedges) {
                inEdgeDataArray = new int[degree.inDegree * (edgeValueConverter != null ? 3 : 1)];
            } else {
                nInedges = degree.inDegree;
            }
            if (!disableOutedges) {
                outEdgeDataArray = new int[degree.outDegree * (edgeValueConverter != null ? 3 : 1)];
            } else {
                nOutedges.set(degree.outDegree);
            }
        }
    }


    public int getId() {
        return this.id;
    }

    public void setDataPtr(ChiPointer vertexPtr) {
        this.vertexPtr = vertexPtr;
    }


    /**
     * Access the value of a vertex
     */
    public VertexValue getValue() {
        return blockManager.dereference(vertexPtr, (BytesToValueConverter<VertexValue>)
                vertexValueConverter);
    }

    /**
     * Set the value of the vertex.
     * @param x new value
     */
    public void setValue(VertexValue x) {
        blockManager.writeValue(vertexPtr, vertexValueConverter, x);
    }


    /**
     * Returns a random out-neighbors vertex id.
     * @return
     */
    public int getRandomOutNeighbor() {
        int i = (int) (Math.random() * numOutEdges());
        if (edgeValueConverter != null) {
            int idx = i * 3;
            return outEdgeDataArray[idx + 2];
        } else {
            return outEdgeDataArray[i];
        }
    }

    /**
     * Returns a random neighbor's vertex id.
     * @return
     */
    public int getRandomNeighbor() {
        if (numEdges() == 0) {
            return -1;
        }
        int i = (int) (Math.random() * numEdges());
        return edge(i).getVertexId();
    }



    public int numOutEdges() {
        return nOutedges.get();
    }


    public int numInEdges() {
        return nInedges;
    }

    /**
     * INTERNAL USE ONLY    (TODO: separate better)
     */
    public void addInEdge(int chunkId, int offset, int vertexId) {
        if (edgeValueConverter != null) {
            int idx = nInedges * 3;
            inEdgeDataArray[idx] = chunkId;
            inEdgeDataArray[idx + 1] = offset;
            inEdgeDataArray[idx + 2] = vertexId;
        } else {
            if (inEdgeDataArray != null)
                inEdgeDataArray[nInedges] = vertexId;
        }
        nInedges++;

    }


    /**
     * INTERNAL USE ONLY  (TODO: separate better)
     */
    public void addOutEdge(int chunkId, int offset,  int vertexId) {
        int tmpOutEdges = nOutedges.addAndGet(1) - 1;
        if (edgeValueConverter != null) {
            int idx = tmpOutEdges * 3;
            outEdgeDataArray[idx] = chunkId;
            outEdgeDataArray[idx + 1] = offset;
            outEdgeDataArray[idx + 2] = vertexId;
        } else {
            if (outEdgeDataArray != null)
                outEdgeDataArray[tmpOutEdges] = vertexId;
        }
    }

    /**
     * Get i'th in-edge
     * @param i
     * @return edge object
     */
    public ChiEdge<EdgeValue> inEdge(int i) {
        if (edgeValueConverter != null) {
            int idx = i * 3;
            return new Edge(new ChiPointer(inEdgeDataArray[idx], inEdgeDataArray[idx + 1]), inEdgeDataArray[idx + 2]);
        } else {
            return new Edge(null, inEdgeDataArray[i]);
        }
    }

    /**
     * Get i'th outedge
     * @param i
     * @return  edge object
     */
    public ChiEdge<EdgeValue>  outEdge(int i) {
        if (edgeValueConverter != null) {
            int idx = i * 3;
            return new Edge(new ChiPointer(outEdgeDataArray[idx], outEdgeDataArray[idx + 1]), outEdgeDataArray[idx + 2]);
        } else {
            return new Edge(null, outEdgeDataArray[i]);
        }
    }

    /**
     * Get vertex-id of the i'th out edge (avoid creating the edge-object).
     * @param i
     */
    public int getOutEdgeId(int i) {
        if (edgeValueConverter != null) {
            int idx = i * 3;
            return outEdgeDataArray[idx + 2];
        } else {
            return outEdgeDataArray[i];
        }
    }

    /**
     * Get i'th edge (in- our out-edge).
     * @param i
     * @return  edge object
     */
    public ChiEdge<EdgeValue> edge(int i) {
        if (i < nInedges) return inEdge(i);
        else return outEdge(i - nInedges);
    }

    /**
     * @return the number of in- and out-edges
     */
    public int numEdges() {
        return nInedges + nOutedges.get();
    }

    /**
     * ONLY ADVANCED USE
     */
    public int[] getOutNeighborArray() {
        if (edgeValueConverter != null) {
            int[] nbrs = new int[numOutEdges()];
            for(int i=0; i<nbrs.length; i++) {
                nbrs[i] = outEdgeDataArray[(i * 3) + 2];
            }
            return nbrs;
        } else {
            return outEdgeDataArray.clone();
        }
    }

    /**
     * Returns the value of i'th outedge (short-cut to outEdge(i)->getValue())
     * @param i
     * @return  the value of i'th outedge (short-cut to outEdge(i)->getValue())
     */
    public EdgeValue getOutEdgeValue(int i) {
        int idx = i * 3;
        return blockManager.dereference(new ChiPointer(outEdgeDataArray[idx], outEdgeDataArray[idx + 1]),
                (BytesToValueConverter<EdgeValue>) edgeValueConverter);
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
