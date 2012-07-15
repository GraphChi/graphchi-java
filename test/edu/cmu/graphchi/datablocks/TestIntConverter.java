package edu.cmu.graphchi.datablocks;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author akyrola
 *         Date: 7/15/12
 */
public class TestIntConverter {

    @Test
    public void testIntConversion() {
        IntConverter conv = new IntConverter();
        byte[] arr = new byte[conv.sizeOf()];
        assertEquals(arr.length, 4);

        int[] tests = new int[] {0, -5, 4, 81912, 839423, 92291, 10000, 2000000000, Integer.MAX_VALUE, Integer.MIN_VALUE};
        for(int x : tests) {
            conv.setValue(arr, x);
            int y = conv.getValue(arr);
            assertEquals(x, y);
        }
    }
}
