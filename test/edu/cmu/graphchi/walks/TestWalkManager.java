package edu.cmu.graphchi.walks;


/**
 * @author Aapo Kyrola
 */
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class TestWalkManager {

    // There's a lot of duplicated code in here to separately test the int and long versions of
    // WalkManager; that could probably be fixed, to just test the parts that are necessary for
    // each one... TODO

    @Test
    public void testIntWalkEncodings() {
        IntWalkManager wmgr = new IntWalkManager(1000, 10000);
        int x = wmgr.encode(3, true, 114);

        System.out.println("X = " + x);

        boolean trackBit = wmgr.trackBit(x);
        int src = wmgr.sourceIdx(x);
        int off = wmgr.off(x);
        assertEquals(3, src);
        assertEquals(true, trackBit);
        assertEquals(114, off);

        x = wmgr.encode(16777200, false, 126);
        trackBit = wmgr.trackBit(x);
        src = wmgr.sourceIdx(x);
        off = wmgr.off(x);
        assertEquals(16777200, src);
        assertEquals(false, trackBit);
        assertEquals(126, off);


        for(int v=0; v<15000000; v+=29) {
            for (int o=0; o<128; o++) {
                x = wmgr.encode(v, true, o);
                int y = wmgr.encode(v, false, o);
                assertEquals(v, wmgr.sourceIdx(x));
                assertEquals(v, wmgr.sourceIdx(y));

                assertEquals(o, wmgr.off(x));
                assertEquals(o, wmgr.off(y));

                assertEquals(true, wmgr.trackBit(x));
                assertEquals(false, wmgr.trackBit(y));
            }
        }


        x = wmgr.encode(16367, true, 0);
        trackBit = wmgr.trackBit(x);
        src = wmgr.sourceIdx(x);
        off = wmgr.off(x);
        assertEquals(16367, src);
        assertEquals(true, trackBit);
        assertEquals(0, off);
    }


    @Test
    public void testLongWalkEncodings() {
        LongWalkManager wmgr = new LongWalkManager(1000, 10000);
        long x = wmgr.encode(3, true, 114);

        System.out.println("X = " + x);

        boolean trackBit = wmgr.trackBit(x);
        int src = wmgr.sourceIdx(x);
        int off = wmgr.off(x);
        assertEquals(3, src);
        assertEquals(true, trackBit);
        assertEquals(114, off);

        x = wmgr.encode(16777200, false, 126);
        trackBit = wmgr.trackBit(x);
        src = wmgr.sourceIdx(x);
        off = wmgr.off(x);
        assertEquals(16777200, src);
        assertEquals(false, trackBit);
        assertEquals(126, off);


        for(int v=0; v<15000000; v+=29) {
            for (int o=0; o<128; o++) {
                x = wmgr.encode(v, true, o);
                long y = wmgr.encode(v, false, o);
                assertEquals(v, wmgr.sourceIdx(x));
                assertEquals(v, wmgr.sourceIdx(y));

                assertEquals(o, wmgr.off(x));
                assertEquals(o, wmgr.off(y));

                assertEquals(true, wmgr.trackBit(x));
                assertEquals(false, wmgr.trackBit(y));
            }
        }


        x = wmgr.encode(16367, true, 0);
        trackBit = wmgr.trackBit(x);
        src = wmgr.sourceIdx(x);
        off = wmgr.off(x);
        assertEquals(16367, src);
        assertEquals(true, trackBit);
        assertEquals(0, off);
    }

    @Test
    public void testIntWalkManager() throws IOException {
        int nvertices = 33333;
        IntWalkManager wmgr = new IntWalkManager(nvertices, 40000);
        int tot = 0;
        for(int j=877; j < 3898; j++) {
            wmgr.addWalkBatch(j, (j % 100) + 10);
            tot += (j % 100) + 10;
        }

        wmgr.initializeWalks();

        assertEquals(tot, wmgr.getTotalWalks());

        // Now get two snapshots
        WalkSnapshot snapshot1 = wmgr.grabSnapshot(890, 1300);
        for(int j=890; j <= 1300; j++) {
            WalkArray vertexwalks = snapshot1.getWalksAtVertex(j, true);
            assertEquals((j % 100) + 10, wmgr.getWalkLength(vertexwalks));

            for(int w : ((IntWalkArray)vertexwalks).getArray()) {
                if (w != -1)
                    assertEquals(false, wmgr.trackBit(w));
            }
        }
        assertEquals(890, snapshot1.getFirstVertex());
        assertEquals(1300, snapshot1.getLastVertex());

        // Next snapshot should be empty
        WalkSnapshot snapshot2 = wmgr.grabSnapshot(890, 1300);
        for(int j=890; j <= 1300; j++) {
            WalkArray vertexwalks = snapshot2.getWalksAtVertex(j, true);
            assertNull(vertexwalks);
        }

        WalkSnapshot snapshot3 = wmgr.grabSnapshot(877, 889);
        for(int j=877; j <= 889; j++) {
            WalkArray vertexwalks = snapshot3.getWalksAtVertex(j, true);
            assertEquals((j % 100) + 10, wmgr.getWalkLength(vertexwalks));
        }

        WalkSnapshot snapshot4 = wmgr.grabSnapshot(877, 889);
        for(int j=877; j <= 889; j++) {
            WalkArray vertexwalks = snapshot4.getWalksAtVertex(j, true);
            assertNull(vertexwalks);
        }

        WalkSnapshot snapshot5 = wmgr.grabSnapshot(1301, 3898);
        for(int j=1301; j < 3898; j++) {
            WalkArray vertexwalks = snapshot5.getWalksAtVertex(j, true);
            assertEquals((j % 100) + 10, wmgr.getWalkLength(vertexwalks));
        }


        // wmgr.dumpToFile(snapshot5, "tmp/snapshot5");


        WalkSnapshot snapshot6 = wmgr.grabSnapshot(1301, 3898);
        for(int j=1301; j < 3898; j++) {
            WalkArray vertexwalks = snapshot6.getWalksAtVertex(j, true);
            assertNull(vertexwalks);
        }

        /* Then update some walks */
        int w = wmgr.encode(41, false, 0);
        wmgr.moveWalk(w, 76, false);
        w = wmgr.encode(88, false, 0);
        wmgr.moveWalk(w, 22098, true);

        WalkSnapshot snapshot7 = wmgr.grabSnapshot(76, 22098);
        WalkArray w1 = snapshot7.getWalksAtVertex(76, true);
        assertEquals(1, wmgr.getWalkLength(w1));
        w = ((IntWalkArray)w1).getArray()[0];
        assertEquals(41, wmgr.sourceIdx(w));
        assertEquals(false, wmgr.trackBit(w));

        WalkArray w2 = snapshot7.getWalksAtVertex(22098, true);
        w = ((IntWalkArray)w2).getArray()[0];
        assertEquals(88, wmgr.sourceIdx(w));
        assertEquals(true, wmgr.trackBit(w));
    }

    @Test
    public void testLongWalkManager() throws IOException {
        int nvertices = 33333;
        LongWalkManager wmgr = new LongWalkManager(nvertices, 40000);
        int tot = 0;
        for(int j=877; j < 3898; j++) {
            wmgr.addWalkBatch(j, (j % 100) + 10);
            tot += (j % 100) + 10;
        }

        wmgr.initializeWalks();

        assertEquals(tot, wmgr.getTotalWalks());

        // Now get two snapshots
        WalkSnapshot snapshot1 = wmgr.grabSnapshot(890, 1300);
        for(int j=890; j <= 1300; j++) {
            WalkArray vertexwalks = snapshot1.getWalksAtVertex(j, true);
            assertEquals((j % 100) + 10, wmgr.getWalkLength(vertexwalks));

            for(long w : ((LongWalkArray)vertexwalks).getArray()) {
                if (w != -1)
                    assertEquals(false, wmgr.trackBit(w));
            }
        }
        assertEquals(890, snapshot1.getFirstVertex());
        assertEquals(1300, snapshot1.getLastVertex());

        // Next snapshot should be empty
        WalkSnapshot snapshot2 = wmgr.grabSnapshot(890, 1300);
        for(int j=890; j <= 1300; j++) {
            WalkArray vertexwalks = snapshot2.getWalksAtVertex(j, true);
            assertNull(vertexwalks);
        }

        WalkSnapshot snapshot3 = wmgr.grabSnapshot(877, 889);
        for(int j=877; j <= 889; j++) {
            WalkArray vertexwalks = snapshot3.getWalksAtVertex(j, true);
            assertEquals((j % 100) + 10, wmgr.getWalkLength(vertexwalks));
        }

        WalkSnapshot snapshot4 = wmgr.grabSnapshot(877, 889);
        for(int j=877; j <= 889; j++) {
            WalkArray vertexwalks = snapshot4.getWalksAtVertex(j, true);
            assertNull(vertexwalks);
        }

        WalkSnapshot snapshot5 = wmgr.grabSnapshot(1301, 3898);
        for(int j=1301; j < 3898; j++) {
            WalkArray vertexwalks = snapshot5.getWalksAtVertex(j, true);
            assertEquals((j % 100) + 10, wmgr.getWalkLength(vertexwalks));
        }


        // wmgr.dumpToFile(snapshot5, "tmp/snapshot5");


        WalkSnapshot snapshot6 = wmgr.grabSnapshot(1301, 3898);
        for(int j=1301; j < 3898; j++) {
            WalkArray vertexwalks = snapshot6.getWalksAtVertex(j, true);
            assertNull(vertexwalks);
        }

        /* Then update some walks */
        long w = wmgr.encode(41, false, 0);
        wmgr.moveWalk(w, 76, false);
        w = wmgr.encode(88, false, 0);
        wmgr.moveWalk(w, 22098, true);

        WalkSnapshot snapshot7 = wmgr.grabSnapshot(76, 22098);
        WalkArray w1 = snapshot7.getWalksAtVertex(76, true);
        assertEquals(1, wmgr.getWalkLength(w1));
        w = ((LongWalkArray)w1).getArray()[0];
        assertEquals(41, wmgr.sourceIdx(w));
        assertEquals(false, wmgr.trackBit(w));

        WalkArray w2 = snapshot7.getWalksAtVertex(22098, true);
        w = ((LongWalkArray)w2).getArray()[0];
        assertEquals(88, wmgr.sourceIdx(w));
        assertEquals(true, wmgr.trackBit(w));
    }
}
