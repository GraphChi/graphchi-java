package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.util.HugeDoubleMatrix;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;


class BiasSgdParams extends ModelParameters {
	public static final String LAMBDA_KEY = "regularization";
	public static final String NUM_LATENT_FACTORS_KEY = "num_latent_factors";
	public static final String STEP_SIZE_KEY = "step_size";
	
	double lambda;	//regularization
	double stepSize; //step size for gradient descent
	int numFeature;	//number of features
	// Number of iterations - Stopping condition. 
	//TODO: Note that may be we can have a better stopping condition based on change in training RMSE.  
	int maxIterations;
	
	HugeDoubleMatrix latentFactors;
	RealVector bias;
	
	public BiasSgdParams(String id, Map<String, String> paramsMap) {
		super(id, paramsMap);
		
		setDefaults();
		
		parseParameters();
		
	}
    public RealVector randomize(long size, double from, double to) {
        Random r = new Random();
        RealVector rv = new ArrayRealVector((int)size); //How about size is really long?
        if(size != (int)size){
        	System.err.println("long size is truncated");
        }
        for(int i = 0 ; i < size ; i++){
        	rv.setEntry(i, (to-from) * r.nextDouble());
        }
        return rv;
    }
	
	public void setDefaults() {
		this.lambda = 0.1;
		this.numFeature = 10;
		this.stepSize = 0.001;
		
		this.maxIterations = 20;
	}
	
	public void parseParameters() {
		if(this.paramsMap.containsKey(LAMBDA_KEY)) {
			this.lambda = Double.parseDouble(this.paramsMap.get(LAMBDA_KEY));
		}
		if(this.paramsMap.containsKey(NUM_LATENT_FACTORS_KEY)) {
			this.numFeature = Integer.parseInt(this.paramsMap.get(NUM_LATENT_FACTORS_KEY));
		}
		if(this.paramsMap.containsKey(STEP_SIZE_KEY)) {
			this.stepSize = Double.parseDouble(this.paramsMap.get(STEP_SIZE_KEY));
		}
		
		//TODO: Read stopping condition (maxIteration currently from parameter file)
	}
	
    void initParameterValues(long size, int D){
        latentFactors = new HugeDoubleMatrix(size, D);

        /* Fill with random data */
        latentFactors.randomize(0.0, 1.0);
        bias = randomize(size,0.0,1.0);        
      }
	
	@Override
	public void serialize(String dir) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deserialize(String file) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public double predict(int originUserId, int originItemId, SparseVector userFeatures,
			SparseVector itemFeatures, SparseVector edgeFeatures,
			DataSetDescription datasetDesc) {		
		// TODO Auto-generated method stub
		double userBias = this.bias.getEntry(originUserId);
		double itemBias = this.bias.getEntry(originItemId);
		RealVector userFactor = this.latentFactors.getRowAsVector(originUserId);
		RealVector itemFactor =  this.latentFactors.getRowAsVector(originItemId);
		return userFactor.dotProduct(itemFactor) + userBias + itemBias;
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
		// TODO Auto-generated method stub
    	this.train_rmse = 0;
        if (this.iterationNum == 0) {
        	params.initParameterValues(ctx.getNumVertices(), params.numFeature);
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
