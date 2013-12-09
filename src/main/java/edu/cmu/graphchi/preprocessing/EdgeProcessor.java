package edu.cmu.graphchi.preprocessing;

import java.util.Map;

/**
 * Interface for objects that translate edge values from string
 * to the value type.
 * @param <ValueType>
 */
public interface EdgeProcessor <ValueType>  {

    public ValueType receiveEdge(int from, int to, String token);
    
}
