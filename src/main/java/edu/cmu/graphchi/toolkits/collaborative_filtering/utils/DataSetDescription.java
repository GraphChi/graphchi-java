package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.File;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * This class encapsulates the description of recommendation system data. It contains
 * certain metadata about the data like number of users, number of items, number of user
 * features etc. and also contains the locations (urls) from which the data can be loaded.
 * This class is used by any class of type InputDataReader to read in input data and represent
 * it as required by the graphchi program.
 * @author mayank
 *
 */

public class DataSetDescription {
	public static final String BASE_FILE_PATH = "baseFilePath";
	
	public static final String RATINGS_LOCATION = "ratingsUrl";
	public static final String USER_FEATURE_LOCATION = "userFeaturesUrl";
	public static final String ITEM_FEATURE_LOCATION = "itemFeaturesUrl";
	public static final String VALIDATION_RATINGS_LOCATION = "validationUrl";
	public static final String TESTING_LOCATION = "testingUrl";
	
	public static final String NUM_USERS = "numUsers";
	public static final String NUM_ITEMS = "numItems";
	public static final String NUM_RATINGS = "numRatings";
	public static final String MEAN_RATING = "meanRating";
	public static final String NUM_VALIDATION_RATINGS = "numValidationRatings";
	
	public static final String NUM_USER_FEATURES = "numUserFeatures";
	public static final String NUM_ITEM_FEATURES = "numItemFeatures";
	public static final String NUM_RATING_FEATURES = "numRatingFeatures";
	
	public static final String MAX_VALUE = "maxval";
	public static final String MIN_VALUE = "minval";

	private String ratingsUrl;
	private String userFeaturesUrl;
	private String itemFeaturesUrl;
	
	private String validationUrl;
	private String testingUrl;
	private long numValRatings;
	
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
	
	public DataSetDescription(String fileName) {
		loadFromJsonFile(fileName);
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
		if(map.get(RATINGS_LOCATION) != null && map.get(RATINGS_LOCATION).length() > 0) {
			this.ratingsUrl = map.get(RATINGS_LOCATION);
		}
		if(map.get(USER_FEATURE_LOCATION) != null) {
			this.userFeaturesUrl = map.get(USER_FEATURE_LOCATION);
		}
		if(map.get(ITEM_FEATURE_LOCATION) != null) {
			this.itemFeaturesUrl = map.get(ITEM_FEATURE_LOCATION);
		}
		if(map.get(VALIDATION_RATINGS_LOCATION) != null) {
			this.validationUrl = map.get(VALIDATION_RATINGS_LOCATION);
		}
		if(map.get(TESTING_LOCATION) != null) {
			this.testingUrl = map.get(TESTING_LOCATION);
		}
		if(map.get(MIN_VALUE) != null) {
			this.minval = Float.parseFloat(map.get(MIN_VALUE));
		}
		if(map.get(MAX_VALUE) != null) {
			this.maxval = Float.parseFloat(map.get(MAX_VALUE));
		}
		if(map.get(NUM_VALIDATION_RATINGS) != null) {
			this.numValRatings = Long.parseLong(map.get(NUM_VALIDATION_RATINGS));
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
	
	public String getRatingsUrl() {
		return ratingsUrl;
	}
	
	public String getUserFeaturesUrl() {
		return userFeaturesUrl;
	}

	public String getItemFeaturesUrl() {
		return itemFeaturesUrl;
	}
	
	public String getValidationUrl() {
		return validationUrl;
	}
	
	public String getTestingUrl() {
		return testingUrl;
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
	
	public void setRatingsUrl(String ratingsUrl) {
		this.ratingsUrl = ratingsUrl;
	}

	public void setUserFeaturesUrl(String userFeaturesUrl) {
		this.userFeaturesUrl = userFeaturesUrl;
	}

	public void setItemFeaturesUrl(String itemFeaturesUrl) {
		this.itemFeaturesUrl = itemFeaturesUrl;
	}

	public void setValidationUrl(String validationUrl) {
		this.validationUrl = validationUrl;
	}
	
	public void setTestingUrl(String testingUrl) {
		this.testingUrl = testingUrl;
	}

	public void setNumUserFeatures(int numUserFeatures) {
		this.numUserFeatures = numUserFeatures;
	}

	public void setNumItemFeatures(int numItemFeatures) {
		this.numItemFeatures = numItemFeatures;
	}

	public void setNumRatingFeatures(int numRatingFeatures) {
		this.numRatingFeatures = numRatingFeatures;
	}

	public void setMaxval(float maxval) {
		this.maxval = maxval;
	}

	public void setMinval(float minval) {
		this.minval = minval;
	}

	public long getNumValRatings() {
		return numValRatings;
	}

	public void setNumValRatings(long numValRatings) {
		this.numValRatings = numValRatings;
	}


	
}
