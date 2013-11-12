package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.cmu.graphchi.io.MatrixMarketDataReader;


//TODO: Is calling Configuration() with default parameters ok?
public class HDFSInputDataReader extends FileInputDataReader {

	public HDFSInputDataReader(DataSetDescription datasetDesc) {
		super(datasetDesc);
		// TODO Auto-generated constructor stub
	}
	
	public HDFSInputDataReader(String dataSetDescFile) {
		super(dataSetDescFile);
	}

	@Override
	public boolean initRatingData() throws IOException, InconsistentDataException {
		Path hdfsPath = new Path(this.ratingFile);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		if(!fs.exists(hdfsPath))
			return false;
		
		this.ratingsReader = new MatrixMarketDataReader(fs.open(hdfsPath));
		this.ratingsReader.init();
		this.metadata.setNumUsers(ratingsReader.numLeft);
		this.metadata.setNumItems(ratingsReader.numRight);
		this.metadata.setNumRatings(ratingsReader.numRatings);
		
		return true;
	}
	
	@Override
	public boolean initUserData()  throws IOException, InconsistentDataException {
		Path hdfsPath = new Path(this.userFile);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		if(!fs.exists(hdfsPath))
			return false;
		
		this.userBr = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
		return true;
	}
	
	@Override
	public boolean initItemData()  throws IOException, InconsistentDataException {
		Path hdfsPath = new Path(this.itemFile);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		if(!fs.exists(hdfsPath))
			return false;
		
		this.itemBr = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
		return true;
	}

}
