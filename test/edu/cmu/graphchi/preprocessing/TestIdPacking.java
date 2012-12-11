package edu.cmu.graphchi.preprocessing;

import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.assertEquals;

/**
 *
 */
public class TestIdPacking {

    @Test
    public void testPacking() {
        Random r = new Random();
        for(int j=0; j < 100000; j++) {
            int i = r.nextInt(Integer.MAX_VALUE);
            long l  = FastSharder.packEdges(j, i);
            assertEquals(j, FastSharder.getFirst(l));
            assertEquals(i, FastSharder.getSecond(l));

            long k  = FastSharder.packEdges(i, j);
            assertEquals(i, FastSharder.getFirst(k));
            assertEquals(j, FastSharder.getSecond(k));
        }
    }
}
