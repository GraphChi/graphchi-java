package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.Serializable;
import java.util.Map;

import gov.sandia.cognition.math.matrix.mtj.SparseVector;

public abstract class ModelParameters implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -372468029741472974L;
	protected boolean serialized;
	protected String id;
	protected Map<String, String> paramsMap;
	
	public ModelParameters(String id, Map<String, String> paramsMap) {
		this.serialized = false;
		this.id = id;
		this.paramsMap = paramsMap;
	}
	
	abstract public void serialize(String dir);
	
	protected void setSerializedTrue(){
		this.serialized = true;
	}
	
	abstract public double predict(int userId, int itemId, 
			SparseVector userFeatures, SparseVector itemFeatures, SparseVector edgeFeatures,
			DataSetDescription datasetDesc);
}
