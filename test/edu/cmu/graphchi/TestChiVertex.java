package edu.cmu.graphchi;

import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.auxdata.VertexDegree;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author akyrola
 *         Date: 7/11/12
 */
public class TestChiVertex {

    @Test
    public void testVertexValue() {
        DataBlockManager blockMgr = new DataBlockManager();

        int blockId = blockMgr.allocateBlock(1024* 1024);

        FloatConverter floatConv = new FloatConverter();

        ChiVertex.edgeValueConverter = floatConv;
        ChiVertex.vertexValueConverter = floatConv;
        ChiVertex.blockManager = blockMgr;
        ChiVertex<Float, Float> vertex = new ChiVertex<Float, Float>(1, new VertexDegree(0, 0));
        assertEquals(vertex.getId(), 1);

        int offset = 1024;
        ChiPointer vertexDataPtr = new ChiPointer(blockId, offset);

        blockMgr.writeValue(vertexDataPtr, floatConv, 3.0f);
        vertex.setDataPtr(vertexDataPtr);

        assertEquals(vertex.getValue(), 3.0f, 1e-15);

        vertex.setValue(999.5f);

        assertEquals(vertex.getValue(), 999.5f, 1e-15f);

        /* Check the data was committed */
        assertEquals(blockMgr.dereference(vertexDataPtr, floatConv), 999.5f, 1e-15f);
    }

    @Test
    public void testInEdges() {
        DataBlockManager blockMgr = new DataBlockManager();

        int blockId = blockMgr.allocateBlock(1024 * 1024);

        FloatConverter floatConv = new FloatConverter();
        int nInedges = 1000;

        ChiVertex.edgeValueConverter = floatConv;
        ChiVertex.vertexValueConverter = floatConv;
        ChiVertex.blockManager = blockMgr;

        ChiVertex<Float, Float> vertex = new ChiVertex<Float, Float>(5, new VertexDegree(nInedges, 0));
        assertEquals(vertex.getId(), 5);

        for(int i=0; i < nInedges; i++) {
            blockMgr.writeValue(new ChiPointer(blockId, i * 4), floatConv, (float) Math.sin(i / 2));
            vertex.addInEdge(blockId, i * 4, i * 7 + 5);
        }
        assertEquals(vertex.numOutEdges(), 0);

        assert(vertex.numInEdges() == nInedges);

        for(int i=nInedges-1; i>=0; i--) {
            ChiEdge<Float> edge = vertex.inEdge(i);
            assertTrue(edge.getVertexId() == i * 7 + 5);
            float val = edge.getValue();
            assertEquals(val, (float) Math.sin(i / 2), 1e-10);
            edge.setValue(i + 2.5f);

            assertEquals(vertex.inEdge(i).getValue(), i + 2.5f, 1e-10);
        }
    }

    @Test
    public void testOutEdges() {
        DataBlockManager blockMgr = new DataBlockManager();

        int blockId = blockMgr.allocateBlock(1024 * 1024);

        FloatConverter floatConv = new FloatConverter();
        int nOutedges = 5559;

        ChiVertex.edgeValueConverter = floatConv;
        ChiVertex.vertexValueConverter = floatConv;
        ChiVertex.blockManager = blockMgr;

        ChiVertex<Float, Float> vertex = new ChiVertex<Float, Float>(5,new VertexDegree(0, nOutedges));
        assertEquals(vertex.getId(), 5);

        assertEquals(vertex.numOutEdges(), 0);

        for(int i=0; i < nOutedges; i++) {
            blockMgr.writeValue(new ChiPointer(blockId, i * 4), floatConv, (float) Math.cos(i / 2));
            vertex.addOutEdge(blockId, i * 4, i * 7 + 5);
            assertEquals(vertex.numOutEdges(), i + 1);
        }
        assertEquals(vertex.numInEdges(), 0);

        assert(vertex.numOutEdges() == nOutedges);

        for(int i=nOutedges-1; i>=0; i--) {
            ChiEdge<Float> edge = vertex.outEdge(i);
            assertTrue(edge.getVertexId() == i * 7 + 5);
            float val = edge.getValue();
            assertEquals(val, (float) Math.cos(i / 2), 1e-10);
            edge.setValue(i + 2.5f);

            assertEquals(vertex.outEdge(i).getValue(), i + 2.5f, 1e-10);
        }
    }


}
