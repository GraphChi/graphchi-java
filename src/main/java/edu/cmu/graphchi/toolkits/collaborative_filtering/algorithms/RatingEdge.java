package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.nio.ByteBuffer;

import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;

public class RatingEdge {
	float observation;
	int[] featureId;
	float[] featureVal;
	
	public RatingEdge() {
		this.observation = 0;
	}
	
	public RatingEdge(float observation, int[] featureId, float[] featureVal) {
		this.observation = observation;
		this.featureId = featureId;
		this.featureVal = featureVal;
	}
}


class RatingEdgeConvertor implements  BytesToValueConverter<RatingEdge> {
	int numFeatures;
	
    public int sizeOf() {
        return 4 + numFeatures*8;
    }
    
    public RatingEdgeConvertor(int numFeatures) {
    	this.numFeatures = numFeatures;
    }
    
    public RatingEdge getValue(byte[] array) {
    	RatingEdge res = null;
    	
    	ByteBuffer buf = ByteBuffer.wrap(array);
    	float obs = buf.getFloat(0);
    	
    	int[] featureIds = new int[this.numFeatures];
    	float[] featureVals = new float[this.numFeatures];
    	
    	for(int i = 0; i < this.numFeatures; i++ ) {
    		featureIds[i] = buf.getInt(4 + i*8);
    		featureVals[i] = buf.getFloat(4 + i*8 + 4);
    	}
    	
    	res = new RatingEdge(obs, featureIds, featureVals);
    	return res;
    }
    
    public void setValue(byte[] array, RatingEdge val) {
    	ByteBuffer buf = ByteBuffer.allocate(sizeOf());
    	buf.putFloat(0, val.observation);
	  
    	for(int i = 0; i < this.numFeatures; i++ ) {
    		buf.putInt(4 + i*8, val.featureId[i]);
    		buf.putFloat(4 + i*8 + 4, val.featureVal[i]);
    	}
    	byte[] a = buf.array();
    	
    	for(int i = 0; i < a.length; i++) {
    		array[i] = a[i];
    	}
    }
}

class RatingEdgeProcessor implements EdgeProcessor<RatingEdge> {

	@Override
	public RatingEdge receiveEdge(int from, int to, String token) {
		if(token == null) {
			return new RatingEdge();
		} else {
			String[] tokens = token.split("\t");
			float obs = Float.parseFloat(tokens[0]);
			
			if(tokens.length == 1) {
				return new RatingEdge(obs, null, null);
			}
			
			int[] featureIds = new int[tokens.length - 1];
			float[] featureVals = new float[tokens.length - 1];
			
			for(int i = 1; i < tokens.length; i++) {
				featureIds[i] = Integer.parseInt(tokens[i].split(":")[0]);
				featureVals[i] = Float.parseFloat(tokens[i].split(":")[1]);
			}
			
			RatingEdge e = new RatingEdge(obs , featureIds, featureVals);
			
			return e;
		}
	}
	
}
