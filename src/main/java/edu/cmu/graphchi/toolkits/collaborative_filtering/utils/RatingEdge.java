package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.nio.ByteBuffer;

import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;

public class RatingEdge {
	public float observation;
	public int[] featureId;
	public float[] featureVal;
	
	public RatingEdge() {
		this.observation = 0;
	}
	
	public RatingEdge(float observation, int[] featureId, float[] featureVal) {
		this.observation = observation;
		this.featureId = featureId;
		this.featureVal = featureVal;
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
