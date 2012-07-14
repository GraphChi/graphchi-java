package edu.cmu.graphchi.datablocks;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author akyrola
 *         Date: 7/10/12
 */
public class TestFloatConverter {

    @Test
    public void testFloatConversion() {
        FloatConverter conv = new FloatConverter();
         byte[] arr = new byte[conv.sizeOf()];
        assertEquals(arr.length, 4);

        float[] tests = new float[] {0.0f, 1.0f, 284392.0f, 1e20f, 8.6e-15f, 1000.0f, 0.25f, 0.000001f, 765.f, 781.f, 6e5f};
        for(float x : tests) {
            conv.setValue(arr, x);
            float y = conv.getValue(arr);
            assertEquals(x, y, 1e-20);
        }
    }
}
