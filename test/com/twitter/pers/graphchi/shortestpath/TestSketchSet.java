package com.twitter.pers.graphchi.shortestpath;

import com.twitter.pers.shortestpath.SketchSet;
import org.junit.Test;

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
    public void testInit() {
        sketchSet = new SketchSet(9);
        sketchSet.selectSeeds(100000000);

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
    public void testSetValues() {
        sketchSet = new SketchSet(9);
        sketchSet.selectSeeds(100000000);
        long current = sketchSet.initialValue();
        sketchSet = new SketchSet(9);
        sketchSet.selectSeeds(100000000);
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
}
