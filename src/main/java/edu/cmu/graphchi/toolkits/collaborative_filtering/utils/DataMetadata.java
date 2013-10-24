package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.util.Map;

public class DataMetadata {
	public static final String NUM_LEFT = "numLeft";
	public static final String NUM_RIGHT = "numRight";
	public static final String NUM_RATINGS = "numRatings";
	public static final String MEAN_RATING = "meanRating";
	
	
	private int numUsers = -1;
	private int numItems = -1;
	private long numRatings = -1;
	private double meanRating = Double.NaN;
	
	public DataMetadata() {
		
	}
	
	public DataMetadata(Map<String, String> map) {
		if(map.get(NUM_LEFT) != null) {
			this.numUsers = Integer.parseInt(map.get(NUM_LEFT));
		} 
		if(map.get(NUM_RIGHT) != null) {
			this.numItems = Integer.parseInt(NUM_RIGHT);
		}
		if(map.get(NUM_RATINGS) != null) {
			this.numRatings = Long.parseLong(NUM_RATINGS);
		}
		if(map.get(meanRating) != null) {
			this.meanRating = Double.parseDouble(MEAN_RATING);
		}
	}
	
	
	public int getNumUsers() {
		return numUsers;
	}
	
	public void setNumUsers(int numUsers) throws InconsistentDataException {
		if(this.numUsers == -1) {
			this.numUsers = numUsers;
			return;
		}
		if(this.numUsers != numUsers ) {
			throw new InconsistentDataException("Number of users set previously " +
					"does not match with current value." + this.numUsers + "!=" + numUsers);
		}
		return;
	}
	
	public int getNumItems() {
		return numItems;
	}
	
	public void setNumItems(int numItems) throws InconsistentDataException {
		if(this.numItems == -1) {
			this.numItems = numItems;
			return;
		}
		if(this.numItems != numItems ) {
			throw new InconsistentDataException("Number of items set previously " +
					"does not match with current value." + this.numItems + "!=" + numItems);
		}
		return;
	}
	
	public long getNumRatings() {
		return numRatings;
	}
	
	public void setNumRatings(long numRatings) throws InconsistentDataException {
		if(this.numRatings == -1) {
			this.numRatings = numRatings;
			return;
		}
		if(this.numRatings != numRatings ) {
			throw new InconsistentDataException("Number of ratings set previously " +
					"does not match with current value." + this.numRatings + "!=" + numRatings);
		}
		return;
	}
	
	public double getMeanRating() {
		return meanRating;
	}
	
	public void setMeanRating(double meanRating) {
		this.meanRating = meanRating;
	}
}
