package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.RatingEdge;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;

public abstract class ModelParameters {

	protected String id;
	protected Map<String, String> paramsMap;
	
	public ModelParameters(String id, Map<String, String> paramsMap) {
		this.id = id;
		this.paramsMap = paramsMap;
	}
	
	public String getId() {
		return this.id;
	}
	
	public Map<String, String> getParamsMap() {
		return this.paramsMap;
	}
	
	public String toJsonString() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(this.paramsMap);
	}
	
	/**
	 * This function serializes the model into the given location.
	 * @param location : Serialize the model into this location (could be local FS, 
	 * HDFS or some other persistent store).
	 */
	abstract public void serialize(String location);
	
	/**
	 * This function de-serializes the model from the given location to an object.
	 * @param location : De-Serialize the model into a Model Parameter object. (could be local FS, 
	 * HDFS or some other persistent store).
	 */
	abstract public void deserialize(String location);
	
	/**
	 * 
	 * @param userId : The original user id (internally while sharding graphchi might translate it)
	 * @param itemId : The original item id (internally while sharding graphchi might translate it)
	 * @param userFeatures : SparseVector representing features of user (Example: Male, Female, Age 10 to 20, etc)
	 * @param itemFeatures : SparseVector representing features of an item. (Example: Action genre, days since release)
	 * @param edgeFeatures : SparseVector representing features of an edge (Example: timestamp)
	 * @param datasetDesc : The description of the dataset.
	 * @return
	 */
	abstract public double predict(int userId, int itemId, 
			SparseVector userFeatures, SparseVector itemFeatures, SparseVector edgeFeatures,
			DataSetDescription datasetDesc);
	
	abstract public int getEstimatedMemoryUsage(DataSetDescription datasetDesc);
	
	
	
}
