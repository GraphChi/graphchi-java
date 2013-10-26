package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.InvalidFileTypeException;
import org.codehaus.jackson.map.ObjectMapper;

import edu.cmu.graphchi.io.MatrixMarketDataReader;

public class FileInputData implements InputData {
	
	public static final String DELIM = "\t| ";
	public static final String FEATURE_DELIM = "\t| ";
	
	String ratingFile;
	String userFile;
	String itemFile;
	DataSetDescription metadata;
	
	private MatrixMarketDataReader ratingsReader;
	
	private String currUserLine;
	private String currItemLine; 
	
	BufferedReader ratingBr = null;
	BufferedReader userBr = null;
	BufferedReader itemBr = null;
	
	public FileInputData(DataSetDescription datasetDesc) {
		this.metadata = datasetDesc;
		
		this.ratingFile = datasetDesc.getRatingsFile();
		this.userFile = datasetDesc.getUserFeaturesFile();
		this.itemFile = datasetDesc.getItemFeaturesFile();
	}
	
	public FileInputData(String dataSetDescFile) {
		DataSetDescription datasetDesc = new DataSetDescription();
		datasetDesc.loadFromJsonFile(dataSetDescFile);
	}
	
	@Override
	public boolean initRatingData() throws IOException, InconsistentDataException {
		File file = new File(this.ratingFile);
		if(!file.exists())
			return false;
		
		this.ratingsReader = new MatrixMarketDataReader(new FileInputStream(file));
		this.ratingsReader.init();
		this.metadata.setNumUsers(ratingsReader.numLeft);
		this.metadata.setNumItems(ratingsReader.numRight);
		this.metadata.setNumRatings(ratingsReader.numRatings);
		
		return true;
	}

	@Override
	public boolean nextRatingData() throws IOException {
		return this.ratingsReader.next();
	}

	@Override
	public int getNextRatingFrom() {
		return this.ratingsReader.getCurrSource();
	}

	@Override
	public int getNextRatingTo() {
		return this.ratingsReader.getCurrDestination();
	}

	@Override
	public float getNextRating() {
		String tok = this.ratingsReader.getCurrEdgeVal().split(DELIM, 2)[0];
		if(tok != null)
			return Float.parseFloat(tok);
		else
			return -1;
	}

	@Override
	public List<Feature> getNextRatingFeatures() {
		String[] tokens = this.ratingsReader.getCurrEdgeVal().split(DELIM, 2);
		if(tokens.length < 2) {
			return null;
		} else {
			return parseFeatures(tokens[1]);
		}
	}

	@Override
	public boolean initUserData()  throws IOException, InconsistentDataException {
		this.userBr = new BufferedReader(new FileReader(new File(this.userFile)));
		return true;
	}

	@Override
	public boolean nextUser() throws IOException{
		String line = progressLine(this.userBr);
		this.currUserLine = line;
		if(line == null) {
			this.userBr = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public int getNextUser() {
		if(this.currUserLine == null) {
			return -1;
		} else {
			return Integer.parseInt(this.currUserLine.split(DELIM)[0]);
		}
	}

	@Override
	public List<Feature> getNextUserFeatures() {
		if(this.currUserLine == null) {
			return null;
		} else {
			String[] tokens = this.currUserLine.split(DELIM,2);
			if(tokens.length < 2) {
				return null;
			} else {
				return parseFeatures(tokens[1]);
			}
		}
	}

	@Override
	public boolean initItemData()  throws IOException, InconsistentDataException {
		this.itemBr = new BufferedReader(new FileReader(new File(this.itemFile)));
		return true;
	}

	@Override
	public boolean nextItem() throws IOException {
		String line = progressLine(this.itemBr);
		this.currItemLine = line;
		if(line == null) {
			this.itemBr = null;
			return false;
		} else {
			return true;
		}
	}
	
	@Override
	public int getNextItem() {
		if(this.currItemLine == null) {
			return -1;
		} else {
			return this.metadata.getNumUsers() + 
				Integer.parseInt(this.currItemLine.split(DELIM)[0]);
		}
	}

	@Override
	public List<Feature> getNextItemFeatures() {
		if(this.currItemLine == null) {
			return null;
		} else {
			String[] tokens = this.currItemLine.split(DELIM,2);
			if(tokens.length < 2) {
				return null;
			} else {
				return parseFeatures(tokens[1]);
			}
		}
	}

	@Override
	public DataSetDescription getDataSetDescription() {
		// TODO Auto-generated method stub
		return this.metadata;
	}
	
	private String progressLine(BufferedReader br) throws IOException {
		String line = null;
		if(br != null) {
			line = br.readLine();
		}
		return line;
	}
	
	List<Feature> parseFeatures(String featureStr) {
		String[] tokens = featureStr.split(DELIM);
		List<Feature> features = new ArrayList<Feature>();
		for(String tok : tokens) {
			int featureId = Integer.parseInt(tok.split(FEATURE_DELIM)[0]);
			float featureVal = Float.parseFloat(tok.split(FEATURE_DELIM)[0]);
			features.add(new Feature(featureId, featureVal));
		}
		return features;
	}

}
