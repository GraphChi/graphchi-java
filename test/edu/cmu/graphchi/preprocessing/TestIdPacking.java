package edu.cmu.graphchi.preprocessing;

import edu.cmu.graphchi.datablocks.FloatConverter;
import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

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

    @Test
    public void testSortWithValues() {
        long[] ids = new long[] {7,4,5,8,2,9};
        float[] valuef = new float[] {7.0f, 4.0f, 5.0f, 8.0f, 2.0f, 9.0f};
        byte[] valuedat = new byte[4 * valuef.length];

        FloatConverter floatConv = new FloatConverter();
        for(int i=0; i < valuef.length; i++) {
            byte[] tmp = new byte[4];
            floatConv.setValue(tmp, valuef[i]);
            System.arraycopy(tmp, 0, valuedat, i * 4, 4);
        }

        FastSharder.sortWithValues(ids, valuedat, 4);

        for(int i=0; i < valuef.length; i++) {
             byte[] tmp = new byte[4];
             System.arraycopy(valuedat, i * 4, tmp, 0, 4);
             float f = floatConv.getValue(tmp);
             assertEquals(ids[i] * 1.0f, f);
             assertTrue(i == 0 || ids[i] > ids[i-1]);
        }
    }
}
