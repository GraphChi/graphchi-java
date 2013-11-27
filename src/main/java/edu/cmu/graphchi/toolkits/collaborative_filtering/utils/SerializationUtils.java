package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.mortbay.io.RuntimeIOException;

import edu.cmu.graphchi.util.HugeDoubleMatrix;

public class SerializationUtils {
	public static final String SERIALIZED_FILE_KEY = "serializedFile";
	public static final String OUTPUTFILE_KEY = "outputFile";
	public static boolean isCommentLine(String line){
		if(line.charAt(0) == '%')
			return true;
		return false;
	}
	public static void serializeParam(String filename, ModelParameters param) throws Exception{
		FileOutputStream fileOut = new FileOutputStream(filename);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(param);
		out.close();
		fileOut.close();
		System.out.printf("Serialized Params is saved in " + filename);
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
					paramsPredict.add(new ModelParametersPrediction(param,outputFile));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeIOException("Could not parse the model description json file: " + serializeJsonFile); 
		}
		return paramsPredict;
	}
}
