package edu.cmu.graphchi.walks;

import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

/**
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public class TestWalkManagerWithPaths {

    @Test
    public void testWalkEncodings() {
        WalkManagerForPaths wmgr = new WalkManagerForPaths(1000);
        long x = wmgr.encode(3, 2, 284);
        int hop = wmgr.hop(x);
        int id = wmgr.walkId(x);
        int off = wmgr.off(x);
        assertEquals(3, id);
        assertEquals(2, hop);
        assertEquals(284, off);

        x = wmgr.encode(878, 0, 999);
        hop = wmgr.hop(x);
        id = wmgr.walkId(x);
        off = wmgr.off(x);
        assertEquals(878, id);
        assertEquals(0, hop);
        assertEquals(999, off);

        x = wmgr.encode(1987000001, 8, 0);
        hop = wmgr.hop(x);
        id = wmgr.walkId(x);
        off = wmgr.off(x);
        assertEquals(1987000001, id);
        assertEquals(8, hop);
        assertEquals(0, off);
    }



    @Test
    public void testWalkManagerWithPaths() throws IOException {
        int nvertices = 33333;
        WalkManagerForPaths wmgr = new WalkManagerForPaths(nvertices);
        int tot = 0;
        for(int j=877; j < 3898; j++) {
            wmgr.addWalkBatch(j, (j % 100) + 10);
            tot += (j % 100) + 10;
        }

        wmgr.initializeWalks();

        assertEquals(tot, wmgr.getTotalWalks());

        // Now get two snapshots
        WalkSnapshotForPaths snapshot1 = wmgr.grabSnapshot(890, 1300);
        for(int j=890; j <= 1300; j++) {
            long[] vertexwalks = snapshot1.getWalksAtVertex(j);
            assertEquals((j % 100) + 10, vertexwalks.length);

            for(long w : vertexwalks) {
                if (w != -1)
                    assertEquals(0, wmgr.hop(w));
            }
        }
        assertEquals(890, snapshot1.getFirstVertex());
        assertEquals(1300, snapshot1.getLastVertex());

        // Next snapshot should be empty
        WalkSnapshotForPaths snapshot2 = wmgr.grabSnapshot(890, 1300);
        for(int j=890; j <= 1300; j++) {
            long[] vertexwalks = snapshot2.getWalksAtVertex(j);
            assertNull(vertexwalks);
        }

        WalkSnapshotForPaths snapshot3 = wmgr.grabSnapshot(877, 889);
        for(int j=877; j <= 889; j++) {
            long[] vertexwalks = snapshot3.getWalksAtVertex(j);
            assertEquals((j % 100) + 10, vertexwalks.length);
        }

        WalkSnapshotForPaths snapshot4 = wmgr.grabSnapshot(877, 889);
        for(int j=877; j <= 889; j++) {
            long[] vertexwalks = snapshot4.getWalksAtVertex(j);
            assertNull(vertexwalks);
        }

        WalkSnapshotForPaths snapshot5 = wmgr.grabSnapshot(1301, 3898);
        for(int j=1301; j < 3898; j++) {
            long[] vertexwalks = snapshot5.getWalksAtVertex(j);
            assertEquals((j % 100) + 10, vertexwalks.length);
        }
       // wmgr.dumpToFile(snapshot5, "snapshot5");


        WalkSnapshotForPaths snapshot6 = wmgr.grabSnapshot(1301, 3898);
        for(int j=1301; j < 3898; j++) {
            long[] vertexwalks = snapshot6.getWalksAtVertex(j);
            assertNull(vertexwalks);
        }

        /* Then update some walks */
        wmgr.updateWalk(88, 22098, 5);
        wmgr.updateWalk(41, 76, 3);

        WalkSnapshotForPaths snapshot7 = wmgr.grabSnapshot(76, 22098);
        long[] w1 = snapshot7.getWalksAtVertex(76);
        assertEquals(1, w1.length);
        long w = w1[0];
        assertEquals(3, wmgr.hop(w));

        long[] w2 = snapshot7.getWalksAtVertex(22098);
        w = w2[0];
        assertEquals(5, wmgr.hop(w));

    }
}
