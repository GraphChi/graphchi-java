package edu.cmu.graphchi.preprocessing;

public interface EdgeProcessor <ValueType>  {


    public ValueType receiveEdge(int from, int to, String token);

}
