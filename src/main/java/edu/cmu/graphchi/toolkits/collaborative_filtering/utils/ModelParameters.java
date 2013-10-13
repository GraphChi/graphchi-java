package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

public abstract class ModelParameters {

	private String id;
	private String paramJson;
	
	public ModelParameters(String id, String json) {
		this.id = id;
		this.paramJson = json;
	}
	
	abstract public void serialize(String dir);
	
	abstract public void deserialize(String file);
	
}
