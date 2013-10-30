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
import edu.cmu.graphchi.datablocks.FloatConverter;
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
	double LAMBDA;	//regularization
	double stepSize; //step size for gradient descent
	int D;	//number of features
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
		this.LAMBDA = 0.1;
		this.D = 10;
		this.stepSize = 0.001;
	}
	
	public void parseParameters() {
		//TODO parse own parameters
	}
	
    void initParameterValues(long size, int D){
        latentFactors = new HugeDoubleMatrix(size, D);

        /* Fill with random data */
        latentFactors.randomize(0f, 1.0f);
        bias = randomize(size,0f,1.0f);        
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
	public double predict(int userId, int itemId, SparseVector userFeatures,
			SparseVector itemFeatures, SparseVector edgeFeatures,
			DataSetDescription datasetDesc) {		
		// TODO Auto-generated method stub
		double userBias = this.bias.getEntry(userId);;
		double itemBias = this.bias.getEntry(itemId);
		RealVector userFactor = this.latentFactors.getRowAsVector(userId);
		RealVector itemFactor =  this.latentFactors.getRowAsVector(itemId);
		return userFactor.dotProduct(itemFactor) + userBias + itemBias;
	}
	
}
public class BiasSgd implements RecommenderAlgorithm {

	private DataSetDescription dataSetDescription;
	private BiasSgdParams params;
	protected Logger logger = ChiLogger.getLogger("BiasSGD");
    double train_rmse = 0.0;
    public BiasSgd(DataSetDescription dataSetDescription , ModelParameters params) {
    	this.dataSetDescription = dataSetDescription;
    	this.params = (BiasSgdParams)params;
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
						+ params.stepSize*(error - params.LAMBDA * params.bias.getEntry(userId)));
				params.bias.setEntry(itemId, params.bias.getEntry(itemId)
						+ params.stepSize*(error - params.LAMBDA * params.bias.getEntry(itemId)));
				RealVector itemFactor = params.latentFactors.getRowAsVector(itemId);
				params.latentFactors.setRow(userId, 
						userFactor.add(
						(itemFactor.mapMultiply(error).subtract(userFactor.mapMultiply(params.LAMBDA))).mapMultiply(params.stepSize))
						.getData());
				params.latentFactors.setRow(itemId,
						itemFactor.add(
						(userFactor.mapMultiply(error).subtract(itemFactor.mapMultiply(params.LAMBDA))).mapMultiply(params.stepSize))
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
        if (ctx.getIteration() == 0) {
        	params.initParameterValues(ctx.getNumVertices(), params.D);
        }
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
        this.train_rmse = Math.sqrt(this.train_rmse / (1.0 * ctx.getNumEdges()));
        this.logger.info("Train RMSE: " + this.train_rmse);
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
		return ctx.getIteration() == ctx.getNumIterations() - 1;
	}

	@Override
	public DataSetDescription getDataSetDescription() {
		// TODO Auto-generated method stub
		return this.dataSetDescription;
	}
	
	
	public static void main(String args[]) throws Exception{
    	ProblemSetup problemSetup = new ProblemSetup(args);
    	
    	DataSetDescription dataDesc = new DataSetDescription();
    	dataDesc.loadFromJsonFile(problemSetup.dataMetadataFile);
    	
    	
    	FastSharder<Integer, RatingEdge> sharder = AggregateRecommender.createSharder(dataDesc.getRatingsUrl(), 
				problemSetup.nShards, 0); 
		IO.convertMatrixMarket(dataDesc.getRatingsUrl(), problemSetup.nShards, sharder);
		List<RecommenderAlgorithm> algosToRun = RecommenderFactory.buildRecommenders(dataDesc, 
				problemSetup.paramFile, null);
    	
		//Just run the first one. It should be BIASSGD.
		if(!(algosToRun.get(0) instanceof BiasSgd)) {
			System.out.println("Please check the parameters file. The first algo listed is not of type BiasSgd");
			System.exit(2);
		}
        GraphChiEngine<Integer, RatingEdge> engine = new GraphChiEngine<Integer, RatingEdge>(dataDesc.getRatingsUrl(), problemSetup.nShards);
		
		GraphChiProgram<Integer, RatingEdge> biasSgd = algosToRun.get(0);
		
        engine.setEdataConverter(new RatingEdgeConvertor(0));
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        engine.setModifiesOutedges(false); // Important optimization
        engine.run(biasSgd, 20);

	}
}
