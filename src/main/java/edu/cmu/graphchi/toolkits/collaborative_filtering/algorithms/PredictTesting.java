package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParametersPrediction;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.SerializationUtils;

public class PredictTesting {
	final static String delim = " ";
	int numUser;
	int numItem;
	String testFilename;
	
	public static void predictOnTest(List<ModelParametersPrediction> modelParams, DataSetDescription dataDesc) throws IOException{
		int numUser = dataDesc.getNumUsers();
		PrintWriter[] writers = new PrintWriter[modelParams.size()];
		for(int i = 0 ; i < writers.length ; i++){
			writers[i] = new PrintWriter(modelParams.get(i).getOutputFile());
		}
		/*if(dataDesc.getTestingUrl() == null){
			System.err.println("No testing file");
			return;
		}*/
		String testFilename = dataDesc.getRatingsUrl();
		BufferedReader br = new BufferedReader(new FileReader(testFilename));
		String line = br.readLine();
		for(int i = 0 ; i < modelParams.size(); i++){
			writers[i].write(line+"\n");
		}
		while( SerializationUtils.isCommentLine(line)){
			line = br.readLine();
			for(int i = 0 ; i < modelParams.size(); i++){
				writers[i].write(line+"\n");
			}
		}
		
		while((line = br.readLine()) != null){	//Line by Line of testing file
			int userId = Integer.parseInt(line.split(delim)[0]);
			int itemId = Integer.parseInt(line.split(delim)[1]);
			int graphChiItemId = itemId +  numUser;
			for(int i = 0 ; i < modelParams.size(); i++){
				ModelParameters params = modelParams.get(i).getParams();			
				double predictedValue = params.predict(userId, graphChiItemId, null, null, null, dataDesc);
				writers[i].write(userId+delim+itemId+delim+predictedValue + "\n");
			}
		}
		br.close();
		for(int i = 0 ; i < writers.length ; i++){
			writers[i].close();
		}
		return;
	}
	public static void main(String args[]){
    	/*ProblemSetup problemSetup = new ProblemSetup(args);
    	
    	DataSetDescription dataDesc = new DataSetDescription();
    	dataDesc.loadFromJsonFile(problemSetup.dataMetadataFile);
		List<RecommenderAlgorithm> algosToRun = RecommenderFactory.buildRecommenders(dataDesc, 
				problemSetup.paramFile, null, problemSetup);
		*/
		ProblemSetup problemSetup = new ProblemSetup(args);    	
    	DataSetDescription dataDesc = new DataSetDescription();
    	dataDesc.loadFromJsonFile(problemSetup.dataMetadataFile);
    	List<ModelParametersPrediction> params = SerializationUtils.deserializeJSON(problemSetup.paramFile);		
		try {
			PredictTesting.predictOnTest(params, dataDesc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
