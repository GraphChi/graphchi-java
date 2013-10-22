package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.BlockRealMatrix;
import org.apache.commons.math.linear.LUDecompositionImpl;
import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;

import edu.cmu.graphchi.ChiEdge;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.util.HugeDoubleMatrix;
import gov.sandia.cognition.math.matrix.Matrix;
import gov.sandia.cognition.math.matrix.mtj.DenseMatrixFactoryMTJ;
import gov.sandia.cognition.statistics.distribution.InverseWishartDistribution;


class PMFParameters extends ModelParameters {
	/*
	 * Graphical Model for Bayesian Probabilistic Matrix Factorization.
	 * From the paper: 
	 * Salakhutdinov and Mnih, Bayesian Probabilistic Matrix Factorization using Markov Chain Monte Carlo. 
	 * in International Conference on Machine Learning, 2008. 
	 *
        nu0, W0                     nu0, W0
          |                           |
          |                           |
          v                           v
    (lambda_V)\                   /(lambda_U)
          |	   \				 /     |
          v     v				v      v
 mu0-->(mu_V)-->(V_j)        (U_i)<--(mu_U)<--mu0
  					\        /
					 \      /
					  v    v
					  (R_ij)
					  	^
					  	|
					  	|
					  alpha
	 */
	
	//Hyper-parameters and Hyperpriors in the above Gaussian Model.
	double alpha;
	
	//For U (users)
	int nu0_U; //degrees of freedom of Gaussion Wishart hyperprior
	double beta0_U; //multiplicative factor to precision matrix in Gaussian-Wishart distribution.
	RealMatrix W0_U; //The scale matrix of Gaussian Wishart hyperprior.
	RealMatrix invW0_U; //Inverse of scale matrix of Gaussian Wishart hyperprior.
	RealVector mu0_U;	//mean hyperprior of the mean of latent factor vectors.
	RealMatrix lambda_U; // The precision matrix (inverse covariance matrix) for V (user's latent factors)
	RealVector mu_U; //The mean vector for U (user's latent factors)
	
	//For V (items)
	int nu0_V; //degrees of freedom of Gaussion Wishart hyperprior
	double beta0_V; //multiplicative factor to precision matrix in Gaussian-Wishart distribution.
	RealMatrix W0_V; //The scale matrix of Gaussian Wishart hyperprior.
	RealMatrix invW0_V; //Inverse of scale matrix of Gaussian Wishart hyperprior.
	RealVector mu0_V;	//mean hyperprior of the mean of latent factor vectors.
	RealMatrix lambda_V; // The precision matrix (inverse covariance matrix) for V (item's latent factors)
	RealVector mu_V; //The mean vector for V (item's latent factors)
	
	HugeDoubleMatrix latentFactors; //list of latent factors (U_i and V_j vectors for different U and V)
	
	//Other variables used while updation of different parameters.
	int N;	//Number of U_i's (users)	
	RealVector sumU;	//SUM U_i
	RealMatrix sumUUT;	//SUM U_i*U_i'
	
	int M;	//Number of U_i's (users)	
	RealVector sumV;	//SUM U_i
	RealMatrix sumVVT;	//SUM U_i*U_i'
	
	int burnInPeriod;	//the burn in period for Gibbs sampling.
	int D;				//number of latent features 
	
	
	public PMFParameters(String id, String json) {
		super(id, json);
		
		setDefaults();
		
		parseParameters(json);
	}
	
	public void setDefaults() {
		this.D = 10;
		this.burnInPeriod = 5;		
		this.alpha = 2.0;
		this.beta0_U = 2.0;
		this.beta0_V = 2.0;
	}
	
	public void parseParameters(String json) {
		//TODO: Implement parsing json string and setting parameters.
	}
	
	public void initParameters(long numVertices) {
		
		//Inititalize hyperparameters for U
		this.nu0_U = this.D; //degrees of freedom equal to latent factors
		this.W0_U = eye(this.D);
		this.invW0_U = eye(this.D);
		this.mu0_U = new ArrayRealVector(this.D);
		
		//Inititalize hyperparameters for V
		this.nu0_V = this.D; //degrees of freedom equal to latent factors
		this.W0_V = eye(this.D);
		this.invW0_V = eye(this.D);
		this.mu0_V = new ArrayRealVector(this.D);
		
		//Initializing precision for U and V to identity.
		this.lambda_U = eye(this.D);
		this.lambda_V = eye(this.D);
		
		//Initializing mean for U and V to 0.
		this.mu_U = new ArrayRealVector(this.D);
		this.mu_V = new ArrayRealVector(this.D);
		
		//Initialize latent factors in memory.
		this.latentFactors = new HugeDoubleMatrix(numVertices, this.D); 
		this.latentFactors.randomize(0, 1);
		
		//Initialize sumU, sumUUT, sumV and sumVVT
		this.sumU = new ArrayRealVector(this.D);
		this.sumV = new ArrayRealVector(this.D);
		this.sumUUT = new BlockRealMatrix(this.D, this.D);
		this.sumVVT = new BlockRealMatrix(this.D, this.D);
	}

	//TODO: Move this to utils.
	public RealMatrix eye(int dim) {
		RealMatrix m = new BlockRealMatrix(dim, dim);
		for(int i = 0; i < dim; i++) {
			m.setEntry(i, i, 1);
		}
		return m;
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

public class PMF implements GraphChiProgram<Integer, EdgeDataType> {

	private static final boolean DEBUG = true;
	PMFProblemSetup setup;
	PMFParameters params;
	
	//Root Mean Squared Error
	double train_rmse;
	protected Logger logger = ChiLogger.getLogger("PMF");
	
	//Constructor
	public PMF(PMFProblemSetup setup, ModelParameters params) {
		this.setup = setup;
		this.params = (PMFParameters) params;
	}
	
	/**
	 * meanU = (SUM U_i)/N
	 * meanS = (SUM (U_i*U_i')/N)
	 * mu0 = (beta0*mu0 + meanU)/(beta0 + N)
	 * beta0 = beta0 + N
	 * nu0 = nu0 + N
	 * W0 = inv( inv(W0) + N*meanS + (beta0*N/(beta0 + N))(mu0 - meanU)*(mu0 - meanU)'
	 * 
	 */
	public void sample_U() {
		//Note, sumU and sumUUT are update in the update function itself. Hence, when
		//sample_U is called after the 1 iteration of all vertices, we already have the sum.
		
		//meanU = (SUM U_i)/N
		RealVector meanU = params.sumU.mapMultiply(1.0/params.N);
		//meanS = (SUM (U_i*U_i')/N)
		RealMatrix meanS = params.sumUUT.scalarMultiply(1.0/params.N);
		
		//mu0 = (beta0*mu0 + meanU)/(beta0 + N)
		RealVector mu0_ = (params.mu0_U.mapMultiply(params.beta0_U).add(meanU)).mapDivide((params.beta0_U + params.N));
		
		double beta0_ = params.beta0_U + params.N;
		int nu0_ = params.nu0_U + params.N;
		
		//W0 = inv( inv(W0) + N*meanS + (beta0*N/(beta0 + N))(mu0 - meanU)*(mu0 - meanU)'
		RealVector tmp = params.mu0_U.subtract(meanU);
		RealMatrix mu0_d_meanU_T = tmp.outerProduct(tmp); 
		mu0_d_meanU_T = mu0_d_meanU_T.scalarMultiply((params.beta0_U*params.N/(params.beta0_U + params.N)));
		RealMatrix invW0_ = params.invW0_U.add(params.sumUUT).add(mu0_d_meanU_T);
		
		//Update all the values.
		params.mu0_U = mu0_;
		params.beta0_U = beta0_;
		params.nu0_U = nu0_;
		params.invW0_U = invW0_;
		params.W0_U = (new LUDecompositionImpl(invW0_)).getSolver().getInverse();
		
		//Sample lambda_U and mu_U from the Gaussian Wishart distribution
		// http://en.wikipedia.org/wiki/Normal-Wishart_distribution#Generating_normal-Wishart_random_variates
		//Draw lambda_U from Wishart distribution with scale matrix w0_U.
		params.lambda_U = sampleWishart(params.invW0_U, params.nu0_U);
		//Compute cov = inv(beta0*lambda) 
		RealMatrix cov = (new LUDecompositionImpl(params.lambda_U.scalarMultiply(params.beta0_U))).getSolver().getInverse();
		//Draw mu_U from multivariate normal dist with mean mu0_U and covariance inv(beta0*lambda)
		MultivariateNormalDistribution dist = new MultivariateNormalDistribution(params.mu0_U.toArray(), 
				cov.getData());
		params.mu_U = new ArrayRealVector(dist.sample());
		
		//Reset the sum of latent factors.
		params.sumU.mapMultiply(0);
		params.sumUUT.scalarMultiply(0);
	}
	
	/**
	 * Sample hyperparameters for V.
	 * TODO: Can common functionalities from sample_U and sample_V be refactored into 1 function?
	 * meanU = (SUM U_i)/N
	 * meanS = (SUM (U_i*U_i')/N)
	 * mu0 = (beta0*mu0 + meanU)/(beta0 + N)
	 * beta0 = beta0 + N
	 * nu0 = nu0 + N
	 * W0 = inv( inv(W0) + N*meanS + (beta0*N/(beta0 + N))(mu0 - meanU)*(mu0 - meanU)'
	 */
	public void sample_V() {
		//Note, sumV and sumVVT are update in the update function itself. Hence, when
		//sample_V is called after the 1 iteration of all vertices, we already have the sum.
		
		//meanV = (SUM V_j)/N
		RealVector meanV = params.sumV.mapMultiply(1.0/params.M);
		//meanS = (SUM (V_j*V_j')/N)
		RealMatrix meanS = params.sumVVT.scalarMultiply(1.0/params.M);
		
		//mu0 = (beta0*mu0 + meanV)/(beta0 + N)
		RealVector mu0_ = (params.mu0_V.mapMultiply(params.beta0_V).add(meanV)).mapDivide((params.beta0_V + params.M));
		
		double beta0_ = params.beta0_V + params.M;
		int nu0_ = params.nu0_V + params.M;
		
		//W0 = inv( inv(W0) + N*meanS + (beta0*N/(beta0 + N))(mu0 - meanU)*(mu0 - meanU)'
		RealVector tmp = params.mu0_V.subtract(meanV);
		RealMatrix mu0_d_meanV_T = tmp.outerProduct(tmp); 
		mu0_d_meanV_T = mu0_d_meanV_T.scalarMultiply((params.beta0_V*params.M/(params.beta0_V + params.M)));
		RealMatrix invW0_ = params.invW0_V.add(params.sumVVT).add(mu0_d_meanV_T);

		//Update all the values.
		params.mu0_V = mu0_;
		params.beta0_V = beta0_;
		params.nu0_V = nu0_;
		params.invW0_V = invW0_;
		params.W0_V = (new LUDecompositionImpl(invW0_)).getSolver().getInverse();
		
		//Sample lambda_V and mu_V from the Gaussian Wishart distribution
		// http://en.wikipedia.org/wiki/Normal-Wishart_distribution#Generating_normal-Wishart_random_variates
		//Draw lambda_V from Wishart distribution with scale matrix w0_U.
		params.lambda_V = sampleWishart(params.invW0_V, params.nu0_V);
		//Compute cov = inv(beta0*lambda) 
		RealMatrix cov = (new LUDecompositionImpl(params.lambda_V.scalarMultiply(params.beta0_V))).getSolver().getInverse();
		//Draw mu_V from multivariate normal dist with mean mu0_V and covariance inv(beta0_V*lambda)
		MultivariateNormalDistribution dist = new MultivariateNormalDistribution(params.mu0_V.toArray(), 
				cov.getData());
		params.mu_V = new ArrayRealVector(dist.sample());
		
		//Reset the sum of latent factors.
		params.sumV.mapMultiply(0);
		params.sumVVT.scalarMultiply(0);
	}
	
	public RealMatrix sampleWishart(RealMatrix invScaleMatrix, int degreesOfFreedom) {
		int n = invScaleMatrix.getRowDimension();
		Matrix invScaleMat = new DenseMatrixFactoryMTJ().createMatrix(n, n);
		for(int i = 0; i < n; i++) {
			for(int j = 0; j < n; j++) {
				invScaleMat.setElement(i, j, invScaleMatrix.getEntry(i, j));
			}
		}
		
		//Sampling from Inverse Wishart distribution using inverse scale matrix should
		//give a sample from the Wishart distribution using the scale matrix.
		//http://en.wikipedia.org/wiki/Inverse-Wishart_distribution
		InverseWishartDistribution dist = new InverseWishartDistribution(invScaleMat, degreesOfFreedom);
		Matrix m = dist.sample(new Random(System.currentTimeMillis()), 1).get(0);
		
		RealMatrix mat = new BlockRealMatrix(n, n);
		for(int i = 0; i < n; i++) {
			for(int j = 0; j < n; j++) {
				mat.setEntry(i, j, m.getElement(i, j));
			}
		}
		
		return mat;
	}
	
	/**
	 * The update function draws a sample for U_i / V_j
	 * p(U_i | R, V, mu_U, lambda_U, alpha) = 
	 * 		[PROD_j(N(R_ij|U_i'*V_j, 1/alpha)] *P(U_i|mu_U, lambda_U) )
	 * 
	 * U_i.lambda = lambda_U + 
	 * 
	 * @param vertex
	 * @param context
	 */
	
	@Override
	public void update(ChiVertex<Integer, EdgeDataType> vertex, GraphChiContext context) {
		boolean isUser = vertex.numOutEdges() > 0;
		double[] nbrPVec = new double[params.D];
		
		//First iteration also computes number of users and number of movies
		if(context.getIteration() == 0) {
			synchronized (this) {
				if(isUser) {
					params.N++;
				} else {
					params.M++;
				}
			}
		}
		
		double[] vData = new double[params.D];
		params.latentFactors.getRow(vertex.getId(), vData);
		
		//Will be updated to store SUM(V_j*R_ij)
		RealVector Xty = new ArrayRealVector(params.D);
		//Will be updated to store SUM(V_j*V_j')
		RealMatrix XtX = new BlockRealMatrix(params.D, params.D);
		
		//Gather data to update the mean and the covariance for the hidden features.	
		for(int i = 0; i < vertex.numEdges(); i++) {
			ChiEdge<EdgeDataType> edge = vertex.edge(i); 
			double observation = edge.getValue().observation;
			
			int nbrId = vertex.edge(i).getVertexId();
			params.latentFactors.getRow(nbrId, nbrPVec);
			////VertexDataType nbrVertex = params.latentFactors.get(nbrId);
			
			//Add V_j*R_ij for this observation.
			////Xty = Xty.add(nbrVertex.pVec.mapMultiply(observation));
			for(int f = 0; f < params.D; f++) {
				double value = Xty.getEntry(f) + nbrPVec[f]*observation;
				Xty.setEntry(f, value);
				
				//Add V_j*V_j' for this
				for(int f2 = 0; f2 < params.D; f2++) {
					value = XtX.getEntry(f, f2) + nbrPVec[f]*nbrPVec[f2];
					XtX.setEntry(f, f2, value);
				}
			}
			
			//Add V_j*V_j' for this 
			////XtX = XtX.add(nbrVertex.pVec.outerProduct(nbrVertex.pVec));
			
		}
		
		RealMatrix lambda_prior = isUser ? params.lambda_U : params.lambda_V;
		RealVector mu_prior = isUser ? params.mu_U : params.mu_V;
		
		RealMatrix precision = lambda_prior.add(XtX.scalarMultiply(params.alpha)); 
		RealMatrix covariance = (new LUDecompositionImpl(precision).getSolver().getInverse());
		
		RealVector tmp = lambda_prior.operate(mu_prior);
		RealVector mean = covariance.operate(Xty.mapMultiply(params.alpha).add(tmp));
		
		//We have the covariance and mean. We can grab a sample from this multivariate
		//normal distribution according to: 
		// http://en.wikipedia.org/wiki/Multivariate_normal_distribution#Drawing_values_from_the_distribution
		//Javadoc:
		MultivariateNormalDistribution dist = new MultivariateNormalDistribution(mean.toArray(), covariance.getData());
		////vData.pVec = new ArrayRealVector(dist.sample());
		vData = dist.sample();
		params.latentFactors.setRow(vertex.getId(), vData);
		
		//Compute contribution of all ratings for this vertex to RMSE.
		if(isUser) {
			for(int i = 0; i < vertex.numEdges(); i++) {
				ChiEdge<EdgeDataType> edge = vertex.edge(i); 
				float observation = edge.getValue().observation;
				int nbrId = vertex.edge(i).getVertexId();
				params.latentFactors.getRow(nbrId, nbrPVec);
				
				//Aggregate the sample and compute rmse if greater than burn_in period
				float prediction = predict(vData, nbrPVec);
				boolean burnedIn = context.getIteration() >= params.burnInPeriod; 
				if(burnedIn) {
					edge.setValue(new EdgeDataType(observation, edge.getValue().aggPred + prediction,
							edge.getValue().count + 1));
					prediction = edge.getValue().aggPred/(float)edge.getValue().count;
				}
			
				synchronized (this) {
					this.train_rmse += (observation - prediction)*(observation - prediction);
				}
			}
		}
		
		//Update Sum of U_i / V_j
		//Should be synchronized? Or Atomic add of vectors?
		RealVector vDataVec = new ArrayRealVector(vData);
		RealMatrix vDataOuterProd = vDataVec.outerProduct(vDataVec);
		synchronized (params) {
			if(isUser) {
					params.sumU = params.sumU.add(vDataVec);
					params.sumUUT = params.sumUUT.add(vDataOuterProd);
			} else {
				params.sumV = params.sumV.add(vDataVec);
				params.sumVVT = params.sumVVT.add(vDataOuterProd);
			}
		}
	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		if (ctx.getIteration() == 0) {
     	   params.initParameters(ctx.getNumVertices());
		}
		// TODO Auto-generated method stub
		synchronized (this) {
			this.train_rmse = 0;
		}
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		// TODO Auto-generated method stub
		//Sample hyperparameters.
		if(ctx.getIteration() == 0) {
			this.logger.info("Number of Users = " + params.N + " and Number of items = " + params.M);
		}
		
		sample_U();
		sample_V();
		
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
	
	public float predict(double[] u, double[] v) {
		double prediction = 0;
		for(int f = 0; f < params.D; f++) {
			prediction += u[f]*v[f];
		}
		
		prediction = Math.max(prediction, this.setup.minval);
		prediction = Math.min(prediction, this.setup.maxval);

		return (float)prediction;
	}
		
	static class PMFProblemSetup extends ProblemSetup {
		//Parameters - hyperpriors
		
		public PMFProblemSetup(String[] args) {
			super(args);
		}
	}
	
	protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Integer, EdgeDataType>(graphName, numShards, null, 
        		new EdgeProcessor<EdgeDataType>() {
		            public EdgeDataType receiveEdge(int from, int to, String token) {
		                return (token == null ? new EdgeDataType() : new EdgeDataType(Float.parseFloat(token),0.0f,0));
		            }
        	}, 
        	new IntConverter(), new EdgeDataTypeConvertor());
    }
	
    public static void main(String[] args) throws Exception {

    	PMFProblemSetup problemSetup = new PMFProblemSetup(args);
    	ModelParameters params = new PMFParameters(problemSetup.getRunId("PMF"), problemSetup.paramJson);

    	FastSharder sharder = PMF.createSharder(problemSetup.training, problemSetup.nShards);
        IO.convertMatrixMarket(problemSetup, sharder);
        
        PMF pmf = new PMF(problemSetup, params);
        
        // Run GraphChi 
        GraphChiEngine<Integer, EdgeDataType> engine = new GraphChiEngine<Integer, EdgeDataType>
        	(problemSetup.training, problemSetup.nShards);
        
        engine.setEdataConverter(new EdgeDataTypeConvertor());
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        //engine.setModifiesOutedges(false);
        engine.run(pmf, 15);
       
        //svdpp.writeOutputMatrices(engine.getVertexIdTranslate());
    }

}

class EdgeDataType {
	float observation;
	float aggPred;
	int count;
	
	public EdgeDataType() {
		this.observation = 0;
		this.aggPred = 0;
		this.count = 0;
	}
	
	public EdgeDataType(float observation, float aggPred, int count) {
		this.observation = observation;
		this.aggPred = aggPred;
		this.count = count;
	}
}


class EdgeDataTypeConvertor implements  BytesToValueConverter<EdgeDataType> {
    public int sizeOf() {
        return 12;
    }
    
    public EdgeDataType getValue(byte[] array) {
    	EdgeDataType res = null;
    	
    	ByteBuffer buf = ByteBuffer.wrap(array);
    	float obs = buf.getFloat(0);
    	float aggPred = buf.getFloat(4);
    	int count = buf.getInt(8);
    	
    	res = new EdgeDataType(obs, aggPred, count);
    	return res;
    }
    
    public void setValue(byte[] array, EdgeDataType val) {
    	  ByteBuffer buf = ByteBuffer.allocate(12);
    	  buf.putFloat(0, val.observation);
    	  buf.putFloat(4, val.aggPred);
    	  buf.putInt(8, val.count);
    	  byte[] a = buf.array();
    	  
    	  for(int i = 0; i < a.length; i++) {
    		  array[i] = a[i];
    	  }
    }
}
