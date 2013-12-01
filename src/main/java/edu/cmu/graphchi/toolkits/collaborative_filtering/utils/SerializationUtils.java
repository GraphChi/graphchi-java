package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.mortbay.io.RuntimeIOException;

import edu.cmu.graphchi.util.HugeDoubleMatrix;

public class SerializationUtils {
	public static final String SERIALIZED_FILE_KEY = "serializedFile";
	public static final String OUTPUTFILE_KEY = "outputFile";
	public static final String ERROR_MEASURE_KEY = "errorMeasure";
	public static boolean isCommentLine(String line){
		if(line.charAt(0) == '%')
			return true;
		return false;
	}
	
	public static String createLocationStr(String prefix, String fileName) {
	    String location = null;
	    if(prefix.startsWith(IO.HDFS_PREFIX)) {
	        String suffix = prefix.substring(IO.HDFS_PREFIX.length()) + "/" + fileName;
	        suffix = suffix.replaceAll("/{2,}", "/");
	        location = IO.HDFS_PREFIX + suffix;
	    } else {
	        if(prefix.contains("//")) {
	            String prePreFix = prefix.split("//")[0] + "//";
	            String suffix = prefix.split("//")[1] + "/" + fileName;
	            suffix = suffix.replaceAll("/{2,}", "/");
	            location = prePreFix + suffix;
	        } else {
	            location = prefix + "/" + fileName;
	            location = location.replaceAll("/{2,}", "/");
	        }
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
	
	public static ModelParameters deserialize(String filename) throws IOException, ClassNotFoundException{
		ModelParameters params = null; 
	    FileInputStream fileIn = new FileInputStream(filename);
	    ObjectInputStream in = new ObjectInputStream(fileIn);
	    params = (ModelParameters) in.readObject();
	    in.close();
	    fileIn.close();
	    params.setSerializedTrue();
	    return params;
	}

	private static ErrorMeasurement getErrorMeasureClass(String errorMeasureString){
		if(errorMeasureString == null || errorMeasureString.equalsIgnoreCase(("RMSE"))){
			return new RmseError();
		}
		else if(errorMeasureString.equalsIgnoreCase(("MAE"))){
			return new MaeError();
		}
		// Default : RMSE
		return new RmseError();
	}

	public static List<ModelParametersPrediction> deserializeJSON(String serializeJsonFile){
		List<ModelParametersPrediction> paramsPredict = new ArrayList<ModelParametersPrediction>();
		try {
			ObjectMapper mapper = new ObjectMapper();
			List<Map<String, String>> models = mapper.readValue(
					new File(serializeJsonFile), TypeFactory.collectionType(List.class, Map.class));
			for(Map<String, String> model : models){
				if(model.containsKey(SERIALIZED_FILE_KEY) && model.containsKey(OUTPUTFILE_KEY)){
					ModelParameters param = deserialize(model.get(SERIALIZED_FILE_KEY));
					String outputFile = model.get(OUTPUTFILE_KEY);
					ErrorMeasurement errorMeasure = getErrorMeasureClass(model.get(ERROR_MEASURE_KEY));
					paramsPredict.add(new ModelParametersPrediction(param, errorMeasure, outputFile));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeIOException("Could not parse the model description json file: " + serializeJsonFile); 
		}
		return paramsPredict;
	}

	public static void main(String[] args) {
	    String prefix = "hdfs://abcd/";
	    String suffix = "/bgd/asdas";
	    
	    System.out.println(createLocationStr(prefix, suffix));
	    
	    prefix = "file://abcd/";
        suffix = "/bgd/asdas";
        System.out.println(createLocationStr(prefix, suffix));
        
        prefix = "foo:///abcd/";
        suffix = "/bgd/asdas";
        System.out.println(createLocationStr(prefix, suffix));
	    
	}
	
}
