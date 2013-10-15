package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.util.logging.Logger;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.util.HugeDoubleMatrix;
class BiasSgdParams extends ModelParameters {
	double LAMBDA;	//regularization
	int D;	//number of features
	HugeDoubleMatrix latentFactors;
	
	public BiasSgdParams(String id, String json) {
		super(id, json);
		
		setDefaults();
		
		parseParameters(json);
		
	}
	
	public void setDefaults() {
		this.LAMBDA = 0.065;
		this.D = 10;
	}
	
	public void parseParameters(String json) {
		
	}
	
    void initParameterValues(long size, int D){
        latentFactors = new HugeDoubleMatrix(size, D);

        /* Fill with random data */
        latentFactors.randomize(0f, 1.0f);
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
public class BiasSgd implements GraphChiProgram<VertexDataType, EdgeDataType> {

	private ProblemSetup problemSetup;
	private ALSParams params;
	protected Logger logger = ChiLogger.getLogger("BiasSGD");
    double train_rmse = 0.0;
    public BiasSgd(ProblemSetup problemSetup, ModelParameters params) {
    	this.problemSetup = problemSetup;
    	this.params = (ALSParams)params;
    }
	@Override
	public void update(ChiVertex<VertexDataType, EdgeDataType> vertex,
			GraphChiContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		// TODO Auto-generated method stub

	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		// TODO Auto-generated method stub
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

}
