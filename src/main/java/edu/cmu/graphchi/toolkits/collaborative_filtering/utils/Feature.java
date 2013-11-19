package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

/**
 * The object of this class represents a particular feature value. 
 * This can be used to represent any user / item feature.
 * Note that most of the times, for compact representation, each feature value 
 * won't be represented by an object of this class. Generally features live in
 * a much more compact form in either VertexDataCache (all user and item features) or
 * in RatingEdge (all the edge features).
 * 
 * @author mayank
 */
public class Feature {
	final int featureId;
	final float featureVal;
	
	public Feature(int featureId, float featureVal) {
		this.featureId = featureId;
		this.featureVal = featureVal;
	}
	
}
