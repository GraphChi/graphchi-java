package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

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
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.util.HugeDoubleMatrix;
class BiasSgdParams extends ModelParameters {
	double LAMBDA;	//regularization
	double stepSize; //step size for gradient descent
	int D;	//number of features
	HugeDoubleMatrix latentFactors;
	RealVector bias;
	
	public BiasSgdParams(String id, String json) {
		super(id, json);
		
		setDefaults();
		
		parseParameters(json);
		
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
		this.LAMBDA = 0.0001;
		this.D = 10;
		this.stepSize = 0.0001;
	}
	
	public void parseParameters(String json) {
		
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
	
}
public class BiasSgd implements GraphChiProgram<Integer, Float> {

	private ProblemSetup problemSetup;
	private BiasSgdParams params;
	protected Logger logger = ChiLogger.getLogger("BiasSGD");
    double train_rmse = 0.0;
    public BiasSgd(ProblemSetup problemSetup, ModelParameters params) {
    	this.problemSetup = problemSetup;
    	this.params = (BiasSgdParams)params;
    }
    public double biasSgdPredict(double userBias, double itemBias, RealVector userFactor, RealVector itemFactor){
    	return userFactor.dotProduct(itemFactor) + userBias + itemBias;
    }
	//@Override
	public void update(ChiVertex<Integer, Float> vertex,
			GraphChiContext context) {
		if(vertex.numEdges() ==0){
			return;
		}
		double squaredError = 0;
		if(vertex.numOutEdges() > 0){ // vertex is an user
			RealVector userFactor = params.latentFactors.getRowAsVector(vertex.getId());
			for(int e = 0 ; e < vertex.numEdges() ; e++){
				float observation = vertex.edge(e).getValue();
				RealVector neighbor = params.latentFactors.getRowAsVector(vertex.edge(e).getVertexId());
				double estimatedRating = biasSgdPredict(params.bias.getEntry(vertex.getId()),
						params.bias.getEntry(vertex.edge(e).getVertexId()),userFactor,neighbor);
				double error = observation - estimatedRating;
				squaredError += Math.pow(error,2);
				params.bias.setEntry(vertex.getId(), params.bias.getEntry(vertex.getId())
						+ params.stepSize*(error - params.LAMBDA * params.bias.getEntry(vertex.getId())));
				params.bias.setEntry(vertex.edge(e).getVertexId(), params.bias.getEntry(vertex.edge(e).getVertexId())
						+ params.stepSize*(error - params.LAMBDA * params.bias.getEntry(vertex.edge(e).getVertexId())));
				RealVector itemFactor = params.latentFactors.getRowAsVector(vertex.edge(e).getVertexId());
				params.latentFactors.setRow(vertex.getId(), 
						userFactor.add(
						(itemFactor.mapMultiply(error).subtract(userFactor.mapMultiply(params.LAMBDA))).mapMultiply(params.stepSize))
						.getData());
				params.latentFactors.setRow(vertex.edge(e).getVertexId(),
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
	public static void main(String args[]) throws Exception{
    	ProblemSetup problemSetup = new ProblemSetup(args);
    	ModelParameters params = new BiasSgdParams(problemSetup.getRunId("BiasSgd"), problemSetup.paramJson);
    	
        BiasSgd biasSgd = new BiasSgd(problemSetup, params);

        IO.convertMatrixMarket(problemSetup);
        
        /* Run GraphChi */
        GraphChiEngine<Integer, Float> engine = new GraphChiEngine<Integer, Float>(problemSetup.training, problemSetup.nShards);
        
        engine.setEdataConverter(new FloatConverter());
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        engine.setModifiesOutedges(false); // Important optimization
        engine.run(biasSgd, 5);

        params.serialize(problemSetup.outputLoc);
	}
}
