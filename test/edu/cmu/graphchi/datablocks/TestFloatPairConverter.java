package edu.cmu.graphchi.datablocks;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author akyrola
 */
public class TestFloatPairConverter {

    @Test
    public void testFloatPairConversion() {
        FloatPairConverter conv = new FloatPairConverter();
        byte[] arr = new byte[conv.sizeOf()];
        assertEquals(arr.length, 8);

        float[] tests = new float[] {0.0f, 1.0f, 284392.0f, 1e20f, 8.6e-15f, 1000.0f, 0.25f, 0.000001f, 765.f, 781.f, 6e5f};
        for(float x : tests) {
            conv.setValue(arr, new FloatPair(x, x * 87));
            FloatPair yp = conv.getValue(arr);
            assertEquals(x, yp.first, 1e-20);
            assertEquals(x * 87, yp.second, 1e-20);
        }
    }
}
