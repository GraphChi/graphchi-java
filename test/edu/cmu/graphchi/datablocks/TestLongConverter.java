package edu.cmu.graphchi.datablocks;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 */
public class TestLongConverter {

    @Test
    public void testLongConversion() {
        LongConverter conv = new LongConverter();
        byte[] arr = new byte[conv.sizeOf()];
        assertEquals(arr.length, 8);

        long[] tests = new long[] {0, -5, 4, 81912, 839423, 92291, 10000, 2000000000,
                Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE,
                        8765432187654l, 80000000000l, 9999999999999l, -432197650000222l};
        for(long x : tests) {
            conv.setValue(arr, x);
            long y = conv.getValue(arr);
            assertEquals(x, y);
        }
    }
}
