package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.util.Map;

public abstract class ModelParameters {

	protected String id;
	protected Map<String, String> paramsMap;
	
	public ModelParameters(String id, Map<String, String> paramsMap) {
		this.id = id;
		this.paramsMap = paramsMap;
	}
	
	abstract public void serialize(String dir);
	
	abstract public void deserialize(String file);
	
}
