package edu.cmu.graphchi.preprocessing;

/**
 * Interface for objects that translate edge values from string
 * to the value type.
 * @param <ValueType>
 */
public interface EdgeProcessor <ValueType>  {

    public ValueType receiveEdge(long from, long to, String token);

}
