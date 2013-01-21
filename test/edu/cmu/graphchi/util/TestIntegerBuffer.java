package edu.cmu.graphchi.util;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test for integer buffer
 */
public class TestIntegerBuffer {

    @Test
    public void testEmpty() {
        IntegerBuffer ib = new IntegerBuffer(10);
        assertEquals(0, ib.toIntArray().length);
    }

    @Test
    public void testBasics() {
        IntegerBuffer ib = new IntegerBuffer(20);
        for(int i=0; i < 2000; i++) {
            ib.add(i);
        }

        int[] arr = ib.toIntArray();
        for(int i=0; i < 2000; i++) {
            assertEquals(i, arr[i]);
        }
        assertEquals(2000, arr.length);
    }
}
