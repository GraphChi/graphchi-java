package edu.cmu.graphchi.toolkits.collaborative_filtering;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.FastSharder;

/**
 * SVD++ algorithm with Stochastic Gradient Descent.
 * This code is based on GraphChi's implementation of SVD++ by Danny Bickson (CMU) 
 * It is based on the algorithm given in the following paper:
 * Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative Filtering Model. 
 * ACM SIGKDD 2008.
 * http://dl.acm.org/citation.cfm?id=1401944 
 *
 * <i>Note:</i>  in this case the vertex values are not used, but as GraphChi does
 * not currently support "no-vertex-values", integer-type is used as placeholder.
 *
 * @author Mayank Mohta, mmohta@andrew.cmu.edu, 2013
 */
public class SVDPP implements GraphChiProgram<Integer, Float>{
	
	private Map<String, String> metadataMap;
	private SVDPPProblemSetup problemSetup;
	protected Logger logger = ChiLogger.getLogger("SVDPP");
	
	private List<SVDPP.VertexDataType> latent_factors_inmem;
	private double train_rmse;
	private double globalMean;
	
	public SVDPP(SVDPPProblemSetup problemSetup, Map<String, String> metadataMap) {
		this.metadataMap = metadataMap;
		this.problemSetup = problemSetup;
		this.globalMean = Double.parseDouble(metadataMap.get("globalMean"));
		this.train_rmse = 0;
	}
	
	static class VertexDataType {
		RealVector pVec;
		RealVector weigths;
		double bias;
		
		public VertexDataType(int D) {
			this.pVec = new ArrayRealVector(D);
			this.weigths = new ArrayRealVector(D);
			this.bias = 0;
			
			for(int i = 0; i < D; i++) {
				this.pVec.setEntry(i, Math.random());
			}
		}
	}
	
	void init_parameters(long size) {
		this.latent_factors_inmem = new ArrayList<SVDPP.VertexDataType>();
		int numFeatures = this.problemSetup.D;
		for(int i = 0; i < size; i++) {
			latent_factors_inmem.add(new VertexDataType(numFeatures));
		}
		
	};
	
	private double svdppPredict(VertexDataType user, VertexDataType movie,
			float observation) {
		// \hat(r_ui) = \mu +
		double prediction = this.globalMean;
		
		// + b_u + b_i
		prediction += user.bias + movie.bias;
		
		// + q_i^T*(p_u + sum y_j/sqrt(|N(u)|))
		prediction += movie.pVec.dotProduct(user.pVec.add(user.weigths));
		
		prediction = Math.max(prediction, this.problemSetup.minval);
		prediction = Math.min(prediction, this.problemSetup.maxval);
		
		return prediction;
	}
	
	@Override
	public void update(ChiVertex<Integer, Float> vertex, GraphChiContext context) {
		if(vertex.numOutEdges() > 0) {
			//User vertex.
			VertexDataType user = latent_factors_inmem.get(vertex.getId());
			
			for(int i = 0; i < this.problemSetup.D; i++) {
				user.weigths.setEntry(i, 0);
			}
			for(int i = 0; i < vertex.numOutEdges(); i++) {
				VertexDataType item = latent_factors_inmem.get(vertex.getOutEdgeId(i));
				user.weigths.add(item.weigths);
			}
			
			//sqrt(|N(u)|)
			double usrNorm = 1.0/Math.sqrt(vertex.numOutEdges());
			user.weigths = user.weigths.mapMultiply(usrNorm);
			
			RealVector step = new ArrayRealVector(this.problemSetup.D);
			
	        // main algorithm, see Koren's paper, just below below equation (16)
	        for(int e=0; e < vertex.numOutEdges(); e++) {
	        	VertexDataType item = latent_factors_inmem.get(vertex.getOutEdgeId(e));
	        	float observation = vertex.getOutEdgeValue(e);
	        	double estScore = svdppPredict(user, item, observation);
	        	
	        	 // e_ui = r_ui - \hat{r_ui}
	        	double err = observation - estScore;
	        	
	        	//q_i = q_i + gamma2*(e_ui*(p_u + sum_j y_j/sqrt(N(U))) - lambda7*q_i)
	        	for (int j=0; j< this.problemSetup.D; j++) { 
	        		double f = item.pVec.getEntry(j) + 
	        			this.problemSetup.itemFactorStep*( 
	        				err*(user.pVec.getEntry(j) + user.weigths.getEntry(j)) - 
	        				this.problemSetup.itemFactorReg*item.pVec.getEntry(j)
	        			);
	        		item.pVec.setEntry(j, f);
	        	}
	        	
	        	//p_u = p_u + gamma2*(e_ui*q_i - lambda7*p_u)
	        	for (int j=0; j< this.problemSetup.D; j++) {
	        		double d = user.pVec.getEntry(j) + 
	        			this.problemSetup.userFactorStep*(
	        				err*item.pVec.getEntry(j) - this.problemSetup.userFactorReg*user.pVec.getEntry(j)
	        			);
	        		user.pVec.setEntry(j, d);
	        	}
	        	
	            //b_i = b_i + gamma1*(e_ui - gmma6 * b_i) 
	        	item.bias += this.problemSetup.itemBiasStep*(err-this.problemSetup.itemBiasReg*item.bias);
	        	//b_u = b_u + gamma1*(e_ui - gamma6 * b_u)
	        	user.bias += this.problemSetup.userBiasStep*(err-this.problemSetup.userBiasReg*user.bias);
	        	
	        	synchronized (this) {
					this.train_rmse += err*err;
				}
	        	
	        	//Batch updation of movie weights
	        	//y_j = y_j  +   gamma2*sqrt|N(u)| * q_i - gamma7 * y_j
	        	//This would change the algorithm - Ask Danny's comments?
	        	step.add(item.pVec.mapMultiply(err*this.problemSetup.itemFactorStep));
	        }
	        
	        //Update item weights.
	        step.mapMultiply(this.problemSetup.itemFactorStep*usrNorm);
	        double regStep = this.problemSetup.itemFactorReg*this.problemSetup.itemFactorStep;
	        
	        for(int i = 0; i < vertex.numOutEdges(); i++) {
				VertexDataType item = latent_factors_inmem.get(vertex.getOutEdgeId(i));
				step = step.subtract(item.weigths.mapMultiply(regStep));
				item.weigths.add(step);
			}
	        
		}
		
	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		 //On first iteration, initialize the vertices in memory.
    	this.train_rmse = 0;
        if (ctx.getIteration() == 0) {
        	   init_parameters(ctx.getNumVertices());
        }		
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		this.train_rmse = Math.sqrt(this.train_rmse / (1.0 * ctx.getNumEdges()));
        this.logger.info("Train RMSE: " + this.train_rmse);
        
        //Reduce all steps.
        this.problemSetup.itemBiasStep *= this.problemSetup.stepDec;
        this.problemSetup.userBiasStep *= this.problemSetup.stepDec;
        this.problemSetup.itemFactorStep *= this.problemSetup.stepDec;
        this.problemSetup.userFactorStep *= this.problemSetup.stepDec;
	}

	@Override
	public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
		
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
	
	
	static class SVDPPProblemSetup extends ProblemSetup {
		double itemFactorStep;	//gamma2
		double itemFactorReg;	//lamda7
		double userFactorStep;	//gamma2
		double userFactorReg;	//lambda7
		double itemBiasReg;		// lamda6
		double userBiasReg;		// lambda6
		double itemBiasStep;	//gamma1
		double userBiasStep;	//gamma1
		double stepDec; 		//
		
		public SVDPPProblemSetup(String[] args) {
			super(args);
			
			this.itemFactorStep = 0.0001;
			this.itemFactorReg = 0.0001;
			this.userFactorStep = 0.0001;
			this.userFactorReg = 0.0001;
			this.itemBiasReg = 0.0001;
			this.itemBiasStep = 0.0001;
			this.userBiasReg = 0.0001;
			this.userBiasReg = 0.0001;
			
			this.stepDec = 0.9;
			
		}
	}
	
    /**
     * Usage: java edu.cmu.graphchi.ALSMatrixFactorization <input-file> <nshards> <D>
     * Normally nshards of 10 or so is fine.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

    	SVDPPProblemSetup problemSetup = new SVDPPProblemSetup(args);

        IO.convert_matrix_market_metadata(problemSetup);
        
        Map<String, String> metadataMap = FastSharder.readMetadata(
        		ChiFilenames.getFilenameMetadata(problemSetup.training, problemSetup.nShards));
        SVDPP svdpp = new SVDPP(problemSetup, metadataMap);
        System.out.println(metadataMap.get("globalMean"));
        
        // Run GraphChi 
        GraphChiEngine<Integer, Float> engine = new GraphChiEngine<Integer, Float>(problemSetup.training, problemSetup.nShards);
        
        engine.setEdataConverter(new FloatConverter());
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        engine.setModifiesOutedges(false); // Important optimization
        engine.run(svdpp, 15);
       
        //svdpp.writeOutputMatrices(engine.getVertexIdTranslate());
    }

}
