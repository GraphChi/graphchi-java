package com.twitter.pers.shortestpath;

/**
 *  @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */
public class SketchBasedDistance {

    public static void main(String[] args) {
        SketchSet sketchSet = new SketchSet(9);
        sketchSet.selectSeeds(1000000000);
    }

}
