package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.util.Map;

import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.RatingEdge;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;

public abstract class ModelParameters {

	protected String id;
	protected Map<String, String> paramsMap;
	
	public ModelParameters(String id, Map<String, String> paramsMap) {
		this.id = id;
		this.paramsMap = paramsMap;
	}
	
	abstract public void serialize(String dir);
	
	abstract public void deserialize(String file);
	
	abstract public double predict(int userId, int itemId, 
			SparseVector userFeatures, SparseVector itemFeatures, SparseVector edgeFeatures,
			DataSetDescription datasetDesc);
}
