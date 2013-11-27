package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Paths;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.cmu.graphchi.util.HugeDoubleMatrix;

public class SerializationUtils {
	public static final String SERIALIZED_FILE_KEY = "serializedFile";
	public static boolean isCommentLine(String line){
		if(line.charAt(0) == '%')
			return true;
		return false;
	}
	
	public static String createLocationStr(String prefix, String fileName) {
	    String location = null;
	    if(prefix.startsWith(IO.HDFS_PREFIX)) {
	        location = IO.HDFS_PREFIX + Paths.get(prefix.substring(IO.HDFS_PREFIX.length()), fileName);
	    } else {
	        location = Paths.get(prefix, fileName).toString();
	    }
	    return location;
	}
	
	public static void serializeParam(String location, ModelParameters param) throws Exception {
	    OutputStream out = null;
	    if(location.startsWith(IO.HDFS_PREFIX)) {
	        FileSystem fs = FileSystem.get(IO.getConf());
	        Path path = new Path(location);
	        out = fs.create(path);
	    } else {
    		out = new FileOutputStream(location);
	    }
	    ObjectOutputStream objOut = null;
	    try {
	        objOut = new ObjectOutputStream(out);
	        objOut.writeObject(param);
	    } finally {
	        if(objOut != null)
	            objOut.close();
	        if(out != null)
	            out.close();
	    }
        System.out.printf("Serialized Params is saved in " + location + "\n");
	}
	
	public static HugeDoubleMatrix deserializeMatrix(String filename) throws IOException{
		final String delim = "\t";
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = br.readLine();
		while( SerializationUtils.isCommentLine(line)){
			line = br.readLine();
		}
		String info[] = line.split(delim);
		for(String s : info)
			System.err.println(s);
		int numRows = Integer.parseInt(info[0]);
		int numDims = Integer.parseInt(info[1]);
		HugeDoubleMatrix latentFactors = new HugeDoubleMatrix(numRows, numDims);
		for(int row = 0 ; row < numRows ; row++){
			for(int d = 0 ; d < numDims ; d++){
				double val = Double.parseDouble(br.readLine());
				latentFactors.setValue(row, d, val);
			}
		}
		br.close();
		return latentFactors;
	}
	public static RealVector deserializeVector(String filename) throws IOException{
		final String delim = "\t";
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = br.readLine();
		while( SerializationUtils.isCommentLine(line)){
			line = br.readLine();
		}
		String info[] = line.split(delim);
		for(String s : info)
			System.err.println(s);
		int numRows = Integer.parseInt(info[0]);
		int numDims = Integer.parseInt(info[1]);
		assert(numDims == 1);
		RealVector bias = new ArrayRealVector(numRows);
		for(int row = 0 ; row < numRows ; row++){
			double val = Double.parseDouble(br.readLine());
			bias.setEntry(row, val);
		}
		br.close();
		return bias;
	}
}
