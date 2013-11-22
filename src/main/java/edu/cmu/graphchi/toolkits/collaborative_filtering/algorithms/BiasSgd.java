package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math.linear.RealVector;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelUtils;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.SerializationUtils;
import edu.cmu.graphchi.util.HugeDoubleMatrix;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;


class BiasSgdParams extends ModelParameters {
	private static final long serialVersionUID = -5531511598859363016L;
	private static final String LAMBDA_KEY = "regularization";
	private static final String NUM_LATENT_FACTORS_KEY = "num_latent_factors";
	private static final String STEP_SIZE_KEY = "step_size";
	
	double lambda;	//regularization
	double stepSize; //step size for gradient descent
	// Number of iterations - Stopping condition. 
	int maxIterations;
	int numFactors;	//number of features
	int numUsers;
	int numItems;
	HugeDoubleMatrix latentFactors;
	RealVector bias;
	
	public BiasSgdParams(String id, Map<String, String> paramsMap) {
		super(id, paramsMap);
		
		setDefaults();
		
		parseParameters();
		
	}

	
	public void setDefaults() {
		this.lambda = 0.1;
		this.numFactors = 10;
		this.stepSize = 0.001;
		
		this.maxIterations = 20;
	}
	
	public void parseParameters() {
		if(this.paramsMap.containsKey(LAMBDA_KEY)) {
			this.lambda = Double.parseDouble(this.paramsMap.get(LAMBDA_KEY));
		}
		if(this.paramsMap.containsKey(NUM_LATENT_FACTORS_KEY)) {
			this.numFactors = Integer.parseInt(this.paramsMap.get(NUM_LATENT_FACTORS_KEY));
		}
		if(this.paramsMap.containsKey(STEP_SIZE_KEY)) {
			this.stepSize = Double.parseDouble(this.paramsMap.get(STEP_SIZE_KEY));
		}
		
		//TODO: Read stopping condition (maxIteration currently from parameter file)
	}
	
    void initParameterValues(long size, int D, DataSetDescription dataMetadata){
    	if(!serialized){
        	numUsers = dataMetadata.getNumUsers();
        	numItems = dataMetadata.getNumItems();
        	latentFactors = new HugeDoubleMatrix(size, D);
            /* Fill with random data */
            latentFactors.randomize(0.0, 1.0);
            bias = ModelUtils.randomize(size,0.0,1.0);
    	}  	
      }
	
	public void serializeMM(String dir) {
		String paramString = "lambda_"+lambda+"_factor_"+numFactors+"_stepSize_"+stepSize;
		String comment = "Latent factors for BiasSGD";		
		IO.mmOutputMatrix(dir+"BiasSGD_latent_factors_"+paramString+".mm" , 0, numUsers + numItems, latentFactors, comment);
		System.err.println("SerializeOver at "+ dir+"BiasSGD_latent_factors_"+paramString+".mm");
		String commentBias = "Bias for BiasSGD";
		IO.mmOutputVector(dir+"BiasSGD_Bias_"+paramString+".mm" , 0, numUsers + numItems, bias, commentBias);
		System.err.println("SerializeOver at "+ dir+"BiasSGD_Bias"+paramString+".mm");
	}
	
	private void deserialzeMM(String filename) throws IOException{
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
		this.latentFactors = new HugeDoubleMatrix(numRows, numDims);
		for(int row = 0 ; row < numRows ; row++){
			for(int d = 0 ; d < numDims ; d++){
				double val = Double.parseDouble(br.readLine());
				this.latentFactors.setValue(row, d, val);
			}
		}
		br.close();
	}

	@Override
	public double predict(int originUserId, int originItemId, SparseVector userFeatures,
			SparseVector itemFeatures, SparseVector edgeFeatures,
			DataSetDescription datasetDesc) {		
		double userBias = this.bias.getEntry(originUserId);
		double itemBias = this.bias.getEntry(originItemId);
		RealVector userFactor = this.latentFactors.getRowAsVector(originUserId);
		RealVector itemFactor =  this.latentFactors.getRowAsVector(originItemId);
		return userFactor.dotProduct(itemFactor) + userBias + itemBias;
	}


	@Override
	public void serialize(String dir) {
	       //TODO: This is not a good way to create a path. Use some library to join into a path
        String filename = dir + this.id;
		try{
			SerializationUtils.serializeParam(filename, this);
		}catch(Exception i){
			System.err.println("Serialization Fails at" + filename);
		}		
	}
	
	public static BiasSgdParams deserialize(String file) throws IOException, ClassNotFoundException {
		BiasSgdParams params = null;
		System.err.println("File:"+file);	  
	    FileInputStream fileIn = new FileInputStream(file);
	    ObjectInputStream in = new ObjectInputStream(fileIn);
	    params = (BiasSgdParams) in.readObject();
	    in.close();
	    fileIn.close();
	    params.setSerializedTrue();
	    return params;
	}
	
	@Override
	public int getEstimatedMemoryUsage(DataSetDescription datasetDesc) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
public class BiasSgd implements RecommenderAlgorithm {

	private DataSetDescription dataSetDescription;
	private BiasSgdParams params;
	protected Logger logger = ChiLogger.getLogger("BiasSGD");
    double train_rmse = 0.0;
    
    int iterationNum ;
    
    public BiasSgd(DataSetDescription dataSetDescription , ModelParameters params) {
    	this.dataSetDescription = dataSetDescription;
    	this.params = (BiasSgdParams)params;
    	this.iterationNum = 0;
    }
	//@Override
	public void update(ChiVertex<Integer, RatingEdge> vertex,
			GraphChiContext context) {
		if(vertex.numEdges() ==0){
			return;
		}
		double squaredError = 0;
		if(vertex.numOutEdges() > 0){ // vertex is an user
			int userId = context.getVertexIdTranslate().backward(vertex.getId());
			RealVector userFactor = params.latentFactors.getRowAsVector(userId);
			for(int e = 0 ; e < vertex.numEdges() ; e++){
				int itemId = context.getVertexIdTranslate().backward(vertex.edge(e).getVertexId());
				float observation = vertex.edge(e).getValue().observation;				
				double estimatedRating = params.predict(userId, itemId, null, null, null, this.dataSetDescription);
				double error = observation - estimatedRating;
				squaredError += Math.pow(error,2);
				params.bias.setEntry(userId, params.bias.getEntry(userId)
						+ params.stepSize*(error - params.lambda * params.bias.getEntry(userId)));
				params.bias.setEntry(itemId, params.bias.getEntry(itemId)
						+ params.stepSize*(error - params.lambda * params.bias.getEntry(itemId)));
				RealVector itemFactor = params.latentFactors.getRowAsVector(itemId);
				params.latentFactors.setRow(userId, 
						userFactor.add(
						(itemFactor.mapMultiply(error).subtract(userFactor.mapMultiply(params.lambda))).mapMultiply(params.stepSize))
						.getData());
				params.latentFactors.setRow(itemId,
						itemFactor.add(
						(userFactor.mapMultiply(error).subtract(itemFactor.mapMultiply(params.lambda))).mapMultiply(params.stepSize))
						.getData());
			}
     	   synchronized (this) {
    		   this.train_rmse += squaredError;
           }
		}		
	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
    	this.train_rmse = 0;
        if (this.iterationNum == 0) {
        	params.initParameterValues(ctx.getNumVertices(), params.numFactors, dataSetDescription );
        }
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
        this.train_rmse = Math.sqrt(this.train_rmse / (1.0 * ctx.getNumEdges()));
        this.logger.info("Train RMSE: " + this.train_rmse);
        this.iterationNum++;
	}

	@Override
	public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
		// TODO Auto-generated method stub

	}

	@Override
	public void endInterval(GraphChiContext ctx, VertexInterval interval) {
		// TODO Auto-generated method stub

	}

	@Override
	public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
		// TODO Auto-generated method stub

	}

	@Override
	public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
		// TODO Auto-generated method stub
	}
	
	@Override
	public ModelParameters getParams() {
		// TODO Auto-generated method stub
		return this.params;
	}

	@Override
	public boolean hasConverged(GraphChiContext ctx) {
		// TODO Auto-generated method stub
		return this.iterationNum == this.params.maxIterations;
	}

	@Override
	public DataSetDescription getDataSetDescription() {
		// TODO Auto-generated method stub
		return this.dataSetDescription;
	}
	
	@Override
	public int getEstimatedMemoryUsage() {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
