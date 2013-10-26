package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.File;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

public class DataSetDescription {
	public static final String RATING_FILE_NAME = "ratingsFile";
	public static final String USER_FEATURE_FILE = "userFeaturesFile";
	public static final String ITEM_FEATURE_FILE = "itemFeaturesFile";
	public static final String VALIDATION_RATING_FILE = "validation";
	
	public static final String NUM_USERS = "numUsers";
	public static final String NUM_ITEMS = "numItems";
	public static final String NUM_RATINGS = "numRatings";
	public static final String MEAN_RATING = "meanRating";
	
	public static final String NUM_USER_FEATURES = "numUserFeatures";
	public static final String NUM_ITEM_FEATURES = "numItemFeatures";
	public static final String NUM_RATING_FEATURES = "numRatingFeatures";
	
	public static final String MAX_VALUE = "maxval";
	public static final String MIN_VALUE = "minval";

	private String ratingsFile;
	private String userFeaturesFile;
	private String itemFeaturesFile;
	private String validationFile;
	
	private int numUsers = -1;
	private int numItems = -1;
	private long numRatings = -1;
	private double meanRating = Double.NaN;

	private int numUserFeatures = 0;
	private int numItemFeatures = 0;
	private int numRatingFeatures = 0;
	
	private float maxval = 100;
	private float minval= -100;
	
	public DataSetDescription() {
		
	}
	
	public void loadFromJsonFile(String fileName) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			Map<String, String> map = (Map<String, String>) mapper.readValue(new File(fileName), Map.class);
			
			loadFromMap(map);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void loadFromMap(Map<String, String> map) {
		if(map.get(NUM_USERS) != null && map.get(NUM_USERS).length() > 0) {
			this.numUsers = Integer.parseInt(map.get(NUM_USERS));
		}
		if(map.get(NUM_ITEMS) != null && map.get(NUM_ITEMS).length() > 0) {
			this.numItems = Integer.parseInt(map.get(NUM_ITEMS));
		}
		if(map.get(NUM_RATINGS) != null && map.get(NUM_RATINGS).length() > 0) {
			this.numRatings = Long.parseLong(map.get(NUM_RATINGS));
		} 
		if(map.get(NUM_USER_FEATURES) != null && map.get(NUM_USER_FEATURES).length() > 0) {
			this.numUserFeatures = Integer.parseInt(map.get(NUM_USER_FEATURES));
		} 
		if(map.get(NUM_ITEM_FEATURES) != null && map.get(NUM_ITEM_FEATURES).length() > 0) {
			this.numItemFeatures = Integer.parseInt(map.get(NUM_ITEM_FEATURES));
;		}
		if(map.get(NUM_RATING_FEATURES) != null && map.get(NUM_RATING_FEATURES).length() > 0) {
			this.numRatingFeatures = Integer.parseInt(map.get(NUM_RATING_FEATURES));
		} 
		if(map.get(RATING_FILE_NAME) != null && map.get(RATING_FILE_NAME).length() > 0) {
			this.ratingsFile = map.get(RATING_FILE_NAME);
		}
		if(map.get(USER_FEATURE_FILE) != null) {
			this.userFeaturesFile = map.get(USER_FEATURE_FILE);
		}
		if(map.get(ITEM_FEATURE_FILE) != null) {
			this.itemFeaturesFile = map.get(ITEM_FEATURE_FILE);
		}
		if(map.get(MIN_VALUE) != null) {
			this.minval = Float.parseFloat(map.get(MIN_VALUE));
		}
		if(map.get(MAX_VALUE) != null) {
			this.maxval = Float.parseFloat(map.get(MAX_VALUE));
		}
		
	}
	
	
	public DataSetDescription(Map<String, String> map) {
		if(map.get(NUM_USERS) != null) {
			this.numUsers = Integer.parseInt(map.get(NUM_USERS));
		} 
		if(map.get(NUM_ITEMS) != null) {
			this.numItems = Integer.parseInt(NUM_ITEMS);
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
	
	public String getRatingsFile() {
		return ratingsFile;
	}
	
	public String getUserFeaturesFile() {
		return userFeaturesFile;
	}

	public String getItemFeaturesFile() {
		return itemFeaturesFile;
	}
	
	public String getValidationFile() {
		return validationFile;
	}

	public int getNumUserFeatures() {
		return numUserFeatures;
	}

	public int getNumItemFeatures() {
		return numItemFeatures;
	}

	public int getNumRatingFeatures() {
		return numRatingFeatures;
	}

	public float getMaxval() {
		return maxval;
	}

	public float getMinval() {
		return minval;
	}
}
