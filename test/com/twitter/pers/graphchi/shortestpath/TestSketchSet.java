package com.twitter.pers.graphchi.shortestpath;

import com.twitter.pers.shortestpath.SketchPath;
import com.twitter.pers.shortestpath.SketchSet;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import static junit.framework.Assert.*;

/**
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
public class TestSketchSet {
    SketchSet sketchSet;

    public void setUp() {

    }

    @Test
    public void testInit()  throws IOException  {
        sketchSet = new SketchSet(9);
        sketchSet.selectSeeds(null, 100000);

        // Everything should start with max distance now
        long current = sketchSet.initialValue();

        for(int seedSet=0; seedSet < 9; seedSet++) {
            int seedIndex = sketchSet.seedIndex(current, seedSet);
            assertEquals("Error on seedSet:" + seedSet, 0, seedIndex);
            int distance = sketchSet.distance(current, seedSet);
            assertEquals("Error on seedSet:" + seedSet, 7, distance);
        }
    }

    @Test
    public void testSetValues() throws IOException {
        sketchSet = new SketchSet(9);
        sketchSet.selectSeeds(null, 1000000);
        long current = sketchSet.initialValue();

        Random random = new Random(260379);

        ArrayList<Integer> chosenSeeds = new ArrayList<Integer>();
        ArrayList<Integer> chosenDistances = new ArrayList<Integer>();


        for(int seedSet=0; seedSet < 9; seedSet++) {
            int nSeeds = (1 << seedSet);
            int newSeed = random.nextInt(nSeeds);
            int newDistance = random.nextInt(6);

            current = sketchSet.encode(current, seedSet, newSeed, newDistance);

            int seedIndex = sketchSet.seedIndex(current, seedSet);
            assertEquals("Error on seedSet:" + seedSet, newSeed, seedIndex);
            int distance = sketchSet.distance(current, seedSet);
            assertEquals("Error on seedSet:" + seedSet, newDistance, distance);

            chosenSeeds.add(newSeed);
            chosenDistances.add(newDistance);
        }

       // Check that the new values remain, and bit arithmetic did not get
       // anything overwritten
        for(int seedSet=0; seedSet < 9; seedSet++) {
            int newSeed = chosenSeeds.get(seedSet);
            int newDistance = chosenDistances.get(seedSet);
            int seedIndex = sketchSet.seedIndex(current, seedSet);
            assertEquals("Error on seedSet:" + seedSet,newSeed, seedIndex);
            int distance = sketchSet.distance(current, seedSet);
            assertEquals("Error on seedSet:" + seedSet,newDistance, distance);
        }
    }

    @Test
    public void testSketchDistance() throws IOException {
        sketchSet = new SketchSet(9);
        sketchSet.selectSeeds(null, 1000000);
        long fromV = sketchSet.initialValue();
        long toV = sketchSet.initialValue();

        // Sketches for seedset 4 match
        fromV = sketchSet.encode(fromV, 5, 7, 2);
        fromV = sketchSet.encode(fromV, 4, 8, 2);
        toV = sketchSet.encode(toV, 5, 14, 3);
        toV = sketchSet.encode(toV, 4, 8, 3);

        SketchPath path = sketchSet.sketchPath(0, 1, fromV, toV);
        assertEquals(2, path.getDistanceFromVertexToSeed());
        assertEquals(3, path.getDistanceFromSeedToDest());
        assertEquals(sketchSet.seeds(4)[8], path.getViaSeed());

        // Tie breaking
        fromV = sketchSet.encode(fromV, 7, 18, 2);
        toV = sketchSet.encode(toV, 7, 18, 3);

        path = sketchSet.sketchPath(0, 1, fromV, toV);
        assertEquals(2, path.getDistanceFromVertexToSeed());
        assertEquals(3, path.getDistanceFromSeedToDest());
        assertEquals(sketchSet.seeds(7)[18], path.getViaSeed());


    }
}
