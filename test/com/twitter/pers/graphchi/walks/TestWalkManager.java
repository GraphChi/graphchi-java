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
        WalkManager wmgr = new WalkManager(1000);
        int x = wmgr.encode(3, 2, 284);
        int hop = wmgr.hop(x);
        int src = wmgr.sourceIdx(x);
        int off = wmgr.off(x);
        assertEquals(3, src);
        assertEquals(2, hop);
        assertEquals(284, off);

        x = wmgr.encode(878, 0, 999);
        hop = wmgr.hop(x);
        src = wmgr.sourceIdx(x);
        off = wmgr.off(x);
        assertEquals(878, src);
        assertEquals(0, hop);
        assertEquals(999, off);

        x = wmgr.encode(16367, 8, 0);
        hop = wmgr.hop(x);
        src = wmgr.sourceIdx(x);
        off = wmgr.off(x);
        assertEquals(16367, src);
        assertEquals(8, hop);
        assertEquals(0, off);
    }

    @Test
    public void testWalkManager() throws IOException {
        int nvertices = 33333;
        WalkManager wmgr = new WalkManager(nvertices);
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
            int[] vertexwalks = snapshot1.getWalksAtVertex(j);
            assertEquals((j % 100) + 10, vertexwalks.length);

            for(int w : vertexwalks) {
                assertEquals(0, wmgr.hop(w));
            }
        }
        assertEquals(890, snapshot1.getFirstVertex());
        assertEquals(1300, snapshot1.getLastVertex());

        // Next snapshot should be empty
        WalkSnapshot snapshot2 = wmgr.grabSnapshot(890, 1300);
        for(int j=890; j <= 1300; j++) {
            int[] vertexwalks = snapshot2.getWalksAtVertex(j);
            assertNull(vertexwalks);
        }

        WalkSnapshot snapshot3 = wmgr.grabSnapshot(877, 889);
        for(int j=877; j <= 889; j++) {
            int[] vertexwalks = snapshot3.getWalksAtVertex(j);
            assertEquals((j % 100) + 10, vertexwalks.length);
        }

        WalkSnapshot snapshot4 = wmgr.grabSnapshot(877, 889);
        for(int j=877; j <= 889; j++) {
            int[] vertexwalks = snapshot4.getWalksAtVertex(j);
            assertNull(vertexwalks);
        }

        WalkSnapshot snapshot5 = wmgr.grabSnapshot(1301, 3898);
        for(int j=1301; j < 3898; j++) {
            int[] vertexwalks = snapshot5.getWalksAtVertex(j);
            assertEquals((j % 100) + 10, vertexwalks.length);
        }
        wmgr.dumpToFile(snapshot5, "/tmp/snapshot5");


        WalkSnapshot snapshot6 = wmgr.grabSnapshot(1301, 3898);
        for(int j=1301; j < 3898; j++) {
            int[] vertexwalks = snapshot6.getWalksAtVertex(j);
            assertNull(vertexwalks);
        }

        /* Then update some walks */
        wmgr.updateWalk(88, 22098, 5);
        wmgr.updateWalk(41, 76, 3);

        WalkSnapshot snapshot7 = wmgr.grabSnapshot(76, 22098);
        int[] w1 = snapshot7.getWalksAtVertex(76);
        assertEquals(1, w1.length);
        int w = w1[0];
        assertEquals(41, wmgr.sourceIdx(w));
        assertEquals(3, wmgr.hop(w));

        int[] w2 = snapshot7.getWalksAtVertex(22098);
        w = w2[0];
        assertEquals(88, wmgr.sourceIdx(w));
        assertEquals(5, wmgr.hop(w));

    }

}
