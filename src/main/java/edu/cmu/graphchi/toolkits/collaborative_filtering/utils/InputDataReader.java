package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface InputDataReader {

	/**
	 * Initializes the rating data from underlying data source. It is only valid to
	 * call nextRatingData() after calling initRatingData. After calling initRatingData,
	 * call to nextRatingData will give the next rating data starting from the first value
	 * and returing the next values on each call until initRatingData is called again or
	 * if the data has been completely read. In the latter case it will return false
	 * @return
	 */
	public boolean initRatingData() throws IOException, InconsistentDataException;
	
	/**
	 * Progresses to the next row of data if initialized and data exists.
	 * 
	 * @return false if there is no next data available of if the rating data is 
	 * not initialized, true otherwise
	 */
	public boolean nextRatingData() throws IOException;
	
	/**
	 * Parses the source vertex id for the rating row read by the last call to nextRatingData. 
	 * @return the id of source vertex if valid row has been read, -1 otherwise.
	 */
	public int getNextRatingFrom();
	
	/**
	 * Parses the destination vertex id for the rating row read by the last call to nextRatingData. 
	 * @return the id of destination vertex if valid row has been read, -1 otherwise.
	 */
	public int getNextRatingTo();
	
	/**
	 * Parses the source vertex id for the rating row read by the last call to nextRatingData. 
	 * @return the id of source vertex if valid row has been read, -1 otherwise.
	 */
	public float getNextRating();
	
	/**
	 * Parses the features related to the rating row read by the last call to nextRatingData and returns
	 * a list of Feature (featureId, featureVal tuple)
	 * @return List<Feature> 
	 */
	public List<Feature> getNextRatingFeatures();
	
	/**
	 * Initializes the user data from underlying data source. It is only valid to
	 * call nextUserData() after calling initUserData. After calling initUserData,
	 * call to nextUserData will give the next user features starting from the first value
	 * and returning the next values on each call until initUserData is called again or
	 * if the data has been completely read. In the latter case it will return false
	 * @return
	 */
	public boolean initUserData() throws IOException, InconsistentDataException;
	
	/**
	 * Progresses to the next row of data if initialized and data exists.
	 * @return false if there is no next data available of if the user data is 
	 * not initialized, true otherwise
	 */
	public boolean nextUser() throws IOException;
	
	/**
	 *  Read in the next user.
	 * @return id of current user if initUserData has been called and it has not reached
	 * the end of dataset.
	 */
	public int getNextUser();
	
	/**
	 * Parse and access the next set of user features
	 * @return: List of features for the current user
	 */
	public List<Feature> getNextUserFeatures();
	
	/**
	 * 
	 * @return
	 */
	public boolean initItemData() throws IOException, InconsistentDataException;
	
	/**
	 * Progresses to the next row of data if initialized and data exists.
	 * @return false if there is no next data available of if the user data is 
	 * not initialized, true otherwise
	 */
	public boolean nextItem() throws IOException;
	
	/**
	 * 
	 * @return
	 */
	public int getNextItem();
	
	/**
	 * 
	 * @return
	 */
	public List<Feature> getNextItemFeatures();
	
	/**
	 * Should return some metadata about the entire dataset. Examples include,
	 * numUsers (source nodes in the bipartite graph), numItems (destination
	 * nodes in the bipartite graph), meanRating etc. Note that not all the values
	 * might be avalaible for metadata, some might be dynamically computed and 
	 * later by algorithm if required.
	 * TODO:
	 * @return 
	 */
	public DataSetDescription getDataSetDescription();
	
}
