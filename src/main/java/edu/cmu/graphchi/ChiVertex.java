package edu.cmu.graphchi;

import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.engine.auxdata.VertexDegree;
import sun.misc.Unsafe;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.lang.reflect.Field;
import java.security.AccessController;
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

    private volatile int nInedges = 0;
    private int[] inEdgeDataArray = null;

    private volatile int nOutedges = 0;
    private int[] outEdgeDataArray = null;

    /* Internal management */
    public boolean parallelSafe = true;

    /* We replicate the behavior of atomic integer to save some memory and improve performance */

    @SuppressWarnings("restriction")
    // http://stackoverflow.com/questions/13003871/how-do-i-get-the-instance-of-sun-misc-unsafe
    private static Unsafe getUnsafe() {
        try {

            Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
            singleoneInstanceField.setAccessible(true);
            return (Unsafe) singleoneInstanceField.get(null);

        } catch (IllegalArgumentException e) {
            throw e;
        } catch (SecurityException e) {
            throw e;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Unsafe unsafe = getUnsafe();
    private static final long valueOffset_nInedges;
    private static final long valueOffset_nOutedges;

    static {
        try {
            valueOffset_nInedges = unsafe.objectFieldOffset
                    (ChiVertex.class.getDeclaredField("nInedges"));
            valueOffset_nOutedges = unsafe.objectFieldOffset
                    (ChiVertex.class.getDeclaredField("nOutedges"));
        } catch (Exception ex) { throw new Error(ex); }
    }

    private ChiPointer vertexPtr;

    public ChiVertex(int id, VertexDegree degree) {
        this.id = id;

        if (degree != null) {
            if (!disableInedges) {
                inEdgeDataArray = new int[degree.inDegree * (edgeValueConverter != null ? 3 : 1)];
            } else {
                nInedges =  degree.inDegree;
            }
            if (!disableOutedges) {
                outEdgeDataArray = new int[degree.outDegree * (edgeValueConverter != null ? 3 : 1)];
            } else {
                nOutedges = degree.outDegree;
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
        return nOutedges;
    }


    public int numInEdges() {
        return nInedges;
    }

    /**
     * INTERNAL USE ONLY    (TODO: separate better)
     */
    public void addInEdge(int chunkId, int offset, int vertexId) {
        int tmpInEdges;
        /* Note: it would be nicer to use AtomicInteger, but I want to save as much memory as possible */
        for (;;) {
            int current = nInedges;
            tmpInEdges = current + 1;
            if (unsafe.compareAndSwapInt(this, valueOffset_nInedges, current, tmpInEdges)) {
                break;
            }
        }

        tmpInEdges--;
        if (edgeValueConverter != null) {
            int idx = tmpInEdges * 3;
            inEdgeDataArray[idx] = chunkId;
            inEdgeDataArray[idx + 1] = offset;
            inEdgeDataArray[idx + 2] = vertexId;
        } else {
            if (inEdgeDataArray != null)
                inEdgeDataArray[tmpInEdges] = vertexId;
        }
    }


    /**
     * INTERNAL USE ONLY  (TODO: separate better)
     */
    public void addOutEdge(int chunkId, int offset,  int vertexId) {
        int tmpOutEdges;
        /* Note: it would be nicer to use AtomicInteger, but I want to save as much memory as possible */
        for (;;) {
            int current = nOutedges;
            tmpOutEdges = current + 1;
            if (unsafe.compareAndSwapInt(this, valueOffset_nOutedges, current, tmpOutEdges)) {
                break;
            }
        }
        tmpOutEdges--;
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
        return nInedges + nOutedges;
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
