package edu.cmu.graphchi.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.mapred.InvalidFileTypeException;

public class MatrixMarketDataReader {
	private static final String DELIM = "\t| "; 

	private InputStream inStream;
	private BufferedReader br;
	private String currLine;
	public int numLeft;
	public int numRight;
	public int numRatings;
	
	public MatrixMarketDataReader(InputStream inStream) {
		this.inStream = inStream;
	}
	
	public boolean init() throws IOException {
		if(inStream == null) {
			return false;
		} else {
			this.br = new BufferedReader(new InputStreamReader(this.inStream));
			String line = this.br.readLine();
			if(!line.contains("matrix coordinate real general")) {
				throw new InvalidFileTypeException("File not in Matrix market format");
			}
			while( (line = this.br.readLine()).startsWith("%") ) {
				//Skip comment lines
			}
			String[] tokens = line.split(DELIM);
			this.numLeft = Integer.parseInt(tokens[0]);
			this.numRight = Integer.parseInt(tokens[1]);
			this.numRatings = Integer.parseInt(tokens[2]);
			return true;
		}
	}
	
	public boolean next() throws IOException {
		String line = this.br.readLine();
		this.currLine = line;
		if(line == null) {
			this.br = null;
			return false;
		} else {
			return true;
		}
	}

	public int getCurrSource() {
		if(this.currLine == null) {
			return -1;
		} else {
			return Integer.parseInt(this.currLine.split(DELIM)[0]);
		}
	}

	public int getCurrDestination() {
		if(this.currLine == null) {
			return -1;
		} else {
			return this.numLeft + Integer.parseInt(this.currLine.split(DELIM)[1]);
		}
	}

	public String getCurrEdgeVal() {
		if(this.currLine == null) {
			return null;
		} else {
			return this.currLine.split(DELIM, 3)[2];
		}
	}
}
