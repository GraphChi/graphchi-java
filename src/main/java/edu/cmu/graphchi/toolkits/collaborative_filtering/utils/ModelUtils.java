package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.util.Random;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;

public class ModelUtils {
    public static RealVector randomize(long size, double from, double to) {
        Random r = new Random();
        RealVector rv = new ArrayRealVector((int)size); //How about size is really long?
        if(size != (int)size){
        	System.err.println("long size is truncated");
        }
        for(int i = 0 ; i < size ; i++){
        	rv.setEntry(i, (to-from) * r.nextDouble());
        }
        return rv;
    }
}
