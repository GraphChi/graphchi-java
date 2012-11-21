package com.twitter.pers.graphchi.walks;


/**
 * @author Aapo Kyrola
 */
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class TestWalkManager {

    @Test
    public void testWalkEncodings() {
        WalkManager wmgr = new WalkManager(1000, 10000);
        int x = wmgr.encode(3, true, 114);

        System.out.println("X = " + x);

        boolean hop = wmgr.hop(x);
        int src = wmgr.sourceIdx(x);
        int off = wmgr.off(x);
        assertEquals(3, src);
        assertEquals(true, hop);
        assertEquals(114, off);

        x = wmgr.encode(16777200, false, 126);
        hop = wmgr.hop(x);
        src = wmgr.sourceIdx(x);
        off = wmgr.off(x);
        assertEquals(16777200, src);
        assertEquals(false, hop);
        assertEquals(126, off);


        for(int v=0; v<15000000; v+=29) {
            for (int o=0; o<128; o++) {
                x = WalkManager.encode(v, true, o);
                int y = WalkManager.encode(v, false, o);
                assertEquals(v, WalkManager.sourceIdx(x));
                assertEquals(v, WalkManager.sourceIdx(y));

                assertEquals(o, WalkManager.off(x));
                assertEquals(o, WalkManager.off(y));

                assertEquals(true, WalkManager.hop(x));
                assertEquals(false, WalkManager.hop(y));
            }
        }


        x = wmgr.encode(16367, true, 0);
        hop = wmgr.hop(x);
        src = wmgr.sourceIdx(x);
        off = wmgr.off(x);
        assertEquals(16367, src);
        assertEquals(true, hop);
        assertEquals(0, off);
    }



    @Test
    public void testWalkManager() throws IOException {
        int nvertices = 33333;
        WalkManager wmgr = new WalkManager(nvertices, 40000);
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
            int[] vertexwalks = snapshot1.getWalksAtVertex(j, true);
            assertEquals((j % 100) + 10, WalkManager.getWalkLength(vertexwalks));

            for(int w : vertexwalks) {
                if (w != -1)
                    assertEquals(false, wmgr.hop(w));
            }
        }
        assertEquals(890, snapshot1.getFirstVertex());
        assertEquals(1300, snapshot1.getLastVertex());

        // Next snapshot should be empty
        WalkSnapshot snapshot2 = wmgr.grabSnapshot(890, 1300);
        for(int j=890; j <= 1300; j++) {
            int[] vertexwalks = snapshot2.getWalksAtVertex(j, true);
            assertNull(vertexwalks);
        }

        WalkSnapshot snapshot3 = wmgr.grabSnapshot(877, 889);
        for(int j=877; j <= 889; j++) {
            int[] vertexwalks = snapshot3.getWalksAtVertex(j, true);
            assertEquals((j % 100) + 10, WalkManager.getWalkLength(vertexwalks));
        }

        WalkSnapshot snapshot4 = wmgr.grabSnapshot(877, 889);
        for(int j=877; j <= 889; j++) {
            int[] vertexwalks = snapshot4.getWalksAtVertex(j, true);
            assertNull(vertexwalks);
        }

        WalkSnapshot snapshot5 = wmgr.grabSnapshot(1301, 3898);
        for(int j=1301; j < 3898; j++) {
            int[] vertexwalks = snapshot5.getWalksAtVertex(j, true);
            assertEquals((j % 100) + 10, WalkManager.getWalkLength(vertexwalks));
        }
        wmgr.dumpToFile(snapshot5, "/tmp/snapshot5");


        WalkSnapshot snapshot6 = wmgr.grabSnapshot(1301, 3898);
        for(int j=1301; j < 3898; j++) {
            int[] vertexwalks = snapshot6.getWalksAtVertex(j, true);
            assertNull(vertexwalks);
        }

        /* Then update some walks */
        wmgr.updateWalk(88, 22098, true);
        wmgr.updateWalk(41, 76, false);

        WalkSnapshot snapshot7 = wmgr.grabSnapshot(76, 22098);
        int[] w1 = snapshot7.getWalksAtVertex(76, true);
        assertEquals(1, WalkManager.getWalkLength(w1));
        int w = w1[0];
        assertEquals(41, wmgr.sourceIdx(w));
        assertEquals(false, wmgr.hop(w));

        int[] w2 = snapshot7.getWalksAtVertex(22098, true);
        w = w2[0];
        assertEquals(88, wmgr.sourceIdx(w));
        assertEquals(true, wmgr.hop(w));

    }

}
