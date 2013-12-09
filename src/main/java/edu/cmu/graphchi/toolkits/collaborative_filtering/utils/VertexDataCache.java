package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.util.List;

import gov.sandia.cognition.math.matrix.mtj.SparseMatrixFactoryMTJ;
import gov.sandia.cognition.math.matrix.mtj.SparseRowMatrix;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;

//Immutable, read-only, in-memory vertex data cache. 
public class VertexDataCache {
	
	//Note that this internally uses an array of int and an array double for each row. 
	//Maybe float instead of double is suitable for our use case. Need to find such impl. or
	//write our own implementation.
	private SparseRowMatrix vertexFeatures;
	
	public VertexDataCache(int numVertices, int maxFeatureId) {
		this.vertexFeatures = (new SparseMatrixFactoryMTJ()).createMatrix(numVertices, maxFeatureId);
	}
	
	public SparseVector getFeatures(int vertexId) {
		return this.vertexFeatures.getRow(vertexId);
	}
	
	public void loadVertexDataCache(InputDataReader data) throws Exception {
	    //TODO: How to handle the case when there is only user or only item data?
		data.initUserData();
		while(data.nextUser()) {
			int userId = data.getCurrUser();
			List<Feature> features = data.getCurrUserFeatures();
			
			for(Feature f : features) {
				this.vertexFeatures.setElement(userId, f.featureId , f.featureVal);
			}
		}
		
		data.initItemData();
		while(data.currItem()) {
			int itemId = data.getCurrItem();
			List<Feature> features = data.getCurrItemFeatures();
			
			for(Feature f : features) {
				this.vertexFeatures.setElement(itemId, f.featureId , f.featureVal);
			}
		}
		
	}
	
	public static VertexDataCache createVertexDataCache(DataSetDescription datasetDesc) {
	    
		if((datasetDesc.getUserFeaturesUrl() != null && datasetDesc.getUserFeaturesUrl().length() > 0) ||
		        (datasetDesc.getItemFeaturesUrl() != null && datasetDesc.getItemFeaturesUrl().length() > 0) ) {
			//Initialize the vertex data cache
			int numVertices = datasetDesc.getNumUsers() + datasetDesc.getNumItems() + 1;
			int maxFeatureId = datasetDesc.getNumUserFeatures() + datasetDesc.getNumItemFeatures()
					+ datasetDesc.getNumRatingFeatures();
			
			VertexDataCache vertexDataCache = new VertexDataCache(numVertices, maxFeatureId);
			
			InputDataReader reader = InputDataReaderFactory.createInputDataReader(datasetDesc);
			
			try {
				vertexDataCache.loadVertexDataCache(reader);
			} catch (Exception e) {
				e.printStackTrace();
				//TODO: Throws Exception instead?
				return null;
			}
			return vertexDataCache;
		} else {
			return null;
		}
	}
	
	/**
	 * 
	 * @param datasetDesc
	 * @return
	 */
	//TODO: No clue how to implement this? We would need average number of features for each vertex?
	public static int getEstimatedMemory(DataSetDescription datasetDesc) {
	    
	    if((datasetDesc.getUserFeaturesUrl() != null && datasetDesc.getUserFeaturesUrl().length() > 0) ||
	            (datasetDesc.getItemFeaturesUrl() != null && datasetDesc.getItemFeaturesUrl().length() > 0) ) {
	    
	        
	    }
	    
	    
	    return 0;
	}

}
