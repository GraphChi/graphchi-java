package edu.cmu.graphchi.preprocessing;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 *
 */
public class TestVertexIdTranslate {

    @Test
    public void testTranslation() {
        for(int verticesInShard=10; verticesInShard < 1000000; verticesInShard += 100000) {
            for(int numShards=(10000000 / verticesInShard + 1); numShards < 100; numShards += 13) {
                VertexIdTranslate trans = new VertexIdTranslate(verticesInShard, numShards);
                for(int j=0; j < 10000000; j+=(1+j)) {
                     int vprime = trans.forward(j);
                     int back = trans.backward(vprime);
                     assertEquals(j, back);
                }

                for(int e=0; e<100; e++) {
                    assertEquals(e, trans.backward(trans.forward(e)));
                }

            }
        }
    }

    @Test
    public void testConstructFromString() {
        VertexIdTranslate tr = new VertexIdTranslate(99999, 44);
        String str = tr.stringRepresentation();

        VertexIdTranslate trReconstr = VertexIdTranslate.fromString(str);

        assertEquals(tr.getNumShards(), trReconstr.getNumShards());
        assertEquals(tr.getVertexIntervalLength(), trReconstr.getVertexIntervalLength());

        for(int j=0; j < 10000000; j+=(1+j)) {
            int v1 = tr.forward(j);
            int v2 = trReconstr.forward(j);
            assertEquals(v1, v2);
        }
    }
}
