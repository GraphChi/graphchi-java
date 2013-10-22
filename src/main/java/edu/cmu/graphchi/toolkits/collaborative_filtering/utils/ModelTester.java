package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.mapred.InvalidFileTypeException;

public class ModelTester {
	private static final String DEFAULT_DELIM = " ";
	
	private String testFile;
	private BufferedReader br;
	private String delim;
	
	private int currUser;
	private int currItem;
	private float currRating;
	private String edgeFeaturesStr;
	
	private int numLeft;
	private int numRight;
	private int numTestCases;
	
	public ModelTester(String testFile) {
		this.testFile = testFile;
		this.delim = DEFAULT_DELIM;
	}
	
	public ModelTester(String testFile, String delim) {
		this.testFile = testFile;
		this.delim = delim;
	}
	
	public void init() {
		try {
			this.br = new BufferedReader(new FileReader(this.testFile));
			String line = br.readLine();
			if(!line.contains("matrix coordinate real general")) {
				throw new InvalidFileTypeException("File not in Matrix market format");
			}
			while( (line = br.readLine()).startsWith("%") ) {
				//Skip comment lines
			}
			String[] tokens = line.split(this.delim);
			this.numLeft = Integer.parseInt(tokens[0]);
			this.numRight = Integer.parseInt(tokens[1]);
			this.numTestCases = Integer.parseInt(tokens[2]);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean next() {
		try {
			String line = br.readLine();
			if(line == null) {
				br.close();
				return false;
			} else {
				String[] tokens = line.split(this.delim, 4);
				this.currUser = Integer.parseInt(tokens[0]) - 1;
				this.currItem = this.numLeft + Integer.parseInt(tokens[1]);
				this.currRating = Float.parseFloat(tokens[2]);
				if(tokens.length > 3) {
					this.edgeFeaturesStr = tokens[3];
				}
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public String getTestFile() {
		return testFile;
	}

	public String getDelim() {
		return delim;
	}

	public int getCurrUser() {
		return currUser;
	}

	public int getCurrItem() {
		return currItem;
	}

	public float getCurrRating() {
		return currRating;
	}

	public String getEdgeFeaturesStr() {
		return edgeFeaturesStr;
	}
	
	public int getNumLeft() {
		return numLeft;
	}

	public int getNumRight() {
		return numRight;
	}

	public int getNumTestCases() {
		return numTestCases;
	}
	
}
