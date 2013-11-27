package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
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
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.RatingEdge;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.SerializationUtils;
import edu.cmu.graphchi.util.HugeDoubleMatrix;
import gov.sandia.cognition.math.matrix.Matrix;
import gov.sandia.cognition.math.matrix.mtj.DenseMatrixFactoryMTJ;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;
import gov.sandia.cognition.statistics.distribution.InverseWishartDistribution;


class PMFParameters extends ModelParameters {
    private static final long serialVersionUID = -1911670561989058906L;
    
    public static final String NUM_LATENT_FACTORS_KEY = "latentFactors";
    public static final String BURN_IN_KEY = "burnIn";
    public static final String MAX_ITERATIONS_KEY = "maxIterations";
    public static final String ALPHA_KEY = "alpha";
    
	 /* Graphical Model for Bayesian Probabilistic Matrix Factorization.
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
	RealVector sumU;	//SUM U_i
	RealMatrix sumUUT;	//SUM U_i*U_i'
	
	RealVector sumV;	//SUM U_i
	RealMatrix sumVVT;	//SUM U_i*U_i'
	
	int burnInPeriod;	//the burn in period for Gibbs sampling.
	int numFactors;		//number of latent features 
	int maxIterations;  //Maximum number of iterations to run this program.
	
	// Contains filenames of all the samples. Note that in PMF, the model is essentially 
	// all the samples of latent factors that were drawn after the burn-in period.
	List<String> sampleFileNames;
	
	public PMFParameters(String id, Map<String, String> paramsMap) {
		super(id, paramsMap);
		
		setDefaults();
		
		parseParameters(paramsMap);
	}
	
	public void setDefaults() {
		this.numFactors = 10;
		this.burnInPeriod = 5;		
		this.alpha = 2.0;
		this.beta0_U = 2.0;
		this.beta0_V = 2.0;
		this.maxIterations = 20;
	}
	
	public void parseParameters(Map<String, String> json) {
        if(this.paramsMap.containsKey(NUM_LATENT_FACTORS_KEY)) {
            this.numFactors = Integer.parseInt(this.paramsMap.get(NUM_LATENT_FACTORS_KEY));
        }
        if(this.paramsMap.containsKey(MAX_ITERATIONS_KEY)) {
            this.maxIterations = Integer.parseInt(this.paramsMap.get(MAX_ITERATIONS_KEY));
        }
        if(this.paramsMap.containsKey(BURN_IN_KEY)) {
            this.burnInPeriod = Integer.parseInt(this.paramsMap.get(BURN_IN_KEY));
        }
        if(this.paramsMap.containsKey(ALPHA_KEY)) {
            this.alpha = Double.parseDouble(this.paramsMap.get(ALPHA_KEY));
        }
	}

	public void initParameters(DataSetDescription datasetDesc) {
	    int numVertices = datasetDesc.getNumUsers() + datasetDesc.getNumItems() + 1; 
		//Inititalize hyperparameters for U
		this.nu0_U = this.numFactors; //degrees of freedom equal to latent factors
		this.W0_U = eye(this.numFactors);
		this.invW0_U = eye(this.numFactors);
		this.mu0_U = new ArrayRealVector(this.numFactors);
		
		//Inititalize hyperparameters for V
		this.nu0_V = this.numFactors; //degrees of freedom equal to latent factors
		this.W0_V = eye(this.numFactors);
		this.invW0_V = eye(this.numFactors);
		this.mu0_V = new ArrayRealVector(this.numFactors);
		
		//Initializing precision for U and V to identity.
		this.lambda_U = eye(this.numFactors);
		this.lambda_V = eye(this.numFactors);
		
		//Initializing mean for U and V to 0.
		this.mu_U = new ArrayRealVector(this.numFactors);
		this.mu_V = new ArrayRealVector(this.numFactors);
		
		//Initialize latent factors in memory.
		this.latentFactors = new HugeDoubleMatrix(numVertices, this.numFactors); 
		this.latentFactors.randomize(0, 1);
		
		//Initialize sumU, sumUUT, sumV and sumVVT
		this.sumU = new ArrayRealVector(this.numFactors);
		this.sumV = new ArrayRealVector(this.numFactors);
		this.sumUUT = new BlockRealMatrix(this.numFactors, this.numFactors);
		this.sumVVT = new BlockRealMatrix(this.numFactors, this.numFactors);
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
        String fileName = Paths.get(dir, id).toString();
        serialize(dir, fileName);
	}
	
	public void serialize(String dir, String fileName) {
	    try{
            SerializationUtils.serializeParam(fileName, this);
        }catch(Exception i){
            System.err.println("Serialization Fails at" + fileName);
        }
	}

    @Override
    public double predict(int userId, int itemId, SparseVector userFeatures,
            SparseVector itemFeatures, SparseVector edgeFeatures,
            DataSetDescription datasetDesc) {
        double[] userData = new double[this.numFactors];
        double[] itemData = new double[this.numFactors];
        
        this.latentFactors.getRow(userId, userData);
        this.latentFactors.getRow(itemId, itemData);
        
        return predict(userData, itemData, datasetDesc);
    }
    
    public double predict(double[] u, double[] v, DataSetDescription datasetDesc) {
        double prediction = 0;
        for(int f = 0; f < this.numFactors; f++) {
            prediction += u[f]*v[f];
        }
        
        prediction = Math.max(prediction, datasetDesc.getMinval());
        prediction = Math.min(prediction, datasetDesc.getMaxval());

        return (float)prediction;
    }

    @Override
    public int getEstimatedMemoryUsage(DataSetDescription datasetDesc) {
        // TODO Auto-generated method stub
        int estimatedMemory = HugeDoubleMatrix.getEstimatedMemory(datasetDesc.getNumUsers()+
                datasetDesc.getNumItems(), this.numFactors );
        //Add 1 Mb of slack (Huge double matrix assigns memory in 1 Mb chunks.)
        estimatedMemory += 1;
        return estimatedMemory;
    }
	
}

public class PMF implements RecommenderAlgorithm {

	private static final boolean DEBUG = true;
	PMFParameters params;
	DataSetDescription datasetDesc;
	
	protected Logger logger = ChiLogger.getLogger("PMF");
	private double train_rmse = 0;
	
	int iterationNum;
	
	private String outputLoc;
	
	//Constructor
	public PMF(DataSetDescription datasetDesc, ModelParameters params, String outputLoc) {
	    this.datasetDesc = datasetDesc;
		this.params = (PMFParameters) params;
		this.outputLoc = outputLoc;
		
		this.iterationNum = 0;
	}
	
	/*
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
		RealVector meanU = params.sumU.mapMultiply(1.0/datasetDesc.getNumUsers());
		//meanS = (SUM (U_i*U_i')/N)
		RealMatrix meanS = params.sumUUT.scalarMultiply(1.0/datasetDesc.getNumUsers());
		
		//mu0 = (beta0*mu0 + meanU)/(beta0 + N)
		RealVector mu0_ = (params.mu0_U.mapMultiply(params.beta0_U).add(meanU)).mapDivide((params.beta0_U + datasetDesc.getNumUsers()));
		
		double beta0_ = params.beta0_U + datasetDesc.getNumUsers();
		int nu0_ = params.nu0_U + datasetDesc.getNumUsers();
		
		//W0 = inv( inv(W0) + N*meanS + (beta0*N/(beta0 + N))(mu0 - meanU)*(mu0 - meanU)'
		RealVector tmp = params.mu0_U.subtract(meanU);
		RealMatrix mu0_d_meanU_T = tmp.outerProduct(tmp); 
		mu0_d_meanU_T = mu0_d_meanU_T.scalarMultiply((params.beta0_U*datasetDesc.getNumUsers()/(params.beta0_U + datasetDesc.getNumUsers())));
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
		RealVector meanV = params.sumV.mapMultiply(1.0/datasetDesc.getNumItems());
		//meanS = (SUM (V_j*V_j')/N)
		RealMatrix meanS = params.sumVVT.scalarMultiply(1.0/datasetDesc.getNumItems());
		
		//mu0 = (beta0*mu0 + meanV)/(beta0 + N)
		RealVector mu0_ = (params.mu0_V.mapMultiply(params.beta0_V).add(meanV)).mapDivide((params.beta0_V + datasetDesc.getNumItems()));
		
		double beta0_ = params.beta0_V + datasetDesc.getNumItems();
		int nu0_ = params.nu0_V + datasetDesc.getNumItems();
		
		//W0 = inv( inv(W0) + N*meanS + (beta0*N/(beta0 + N))(mu0 - meanU)*(mu0 - meanU)'
		RealVector tmp = params.mu0_V.subtract(meanV);
		RealMatrix mu0_d_meanV_T = tmp.outerProduct(tmp); 
		mu0_d_meanV_T = mu0_d_meanV_T.scalarMultiply((params.beta0_V*datasetDesc.getNumItems()/(params.beta0_V + datasetDesc.getNumItems())));
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

	public void update(ChiVertex<Integer, RatingEdge> vertex, GraphChiContext context) {
		boolean isUser = vertex.numOutEdges() > 0;
		double[] nbrPVec = new double[params.numFactors];
		
		double[] vData = new double[params.numFactors];
		int sourceId = context.getVertexIdTranslate().backward(vertex.getId());
		params.latentFactors.getRow(sourceId, vData);
		
		//Will be updated to store SUM(V_j*R_ij)
		RealVector Xty = new ArrayRealVector(params.numFactors);
		//Will be updated to store SUM(V_j*V_j')
		RealMatrix XtX = new BlockRealMatrix(params.numFactors, params.numFactors);
		
		//Gather data to update the mean and the covariance for the hidden features.	
		for(int i = 0; i < vertex.numEdges(); i++) {
			RatingEdge edge = vertex.edge(i).getValue(); 
			double observation = edge.observation;
			
			int nbrId = context.getVertexIdTranslate().backward(vertex.edge(i).getVertexId());
			params.latentFactors.getRow(nbrId, nbrPVec);
			////VertexDataType nbrVertex = params.latentFactors.get(nbrId);
			
			//Add V_j*R_ij for this observation.
			////Xty = Xty.add(nbrVertex.pVec.mapMultiply(observation));
			for(int f = 0; f < params.numFactors; f++) {
				double value = Xty.getEntry(f) + nbrPVec[f]*observation;
				Xty.setEntry(f, value);
				
				//Add V_j*V_j' for this
				for(int f2 = 0; f2 < params.numFactors; f2++) {
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
		params.latentFactors.setRow(sourceId, vData);
		
		//Compute contribution of all ratings for this vertex to RMSE.
		if(isUser) {
			for(int i = 0; i < vertex.numEdges(); i++) {
				ChiEdge<RatingEdge> edge = vertex.edge(i);
				RatingEdge pmfRatingEdge = edge.getValue();
				float observation = pmfRatingEdge.observation;
				int nbrId = context.getVertexIdTranslate().backward(vertex.edge(i).getVertexId());
				params.latentFactors.getRow(nbrId, nbrPVec);
				
				//Aggregate the sample and compute rmse if greater than burn_in period
				double prediction = this.params.predict(vData, nbrPVec, datasetDesc);
				double squaredError = (observation - prediction)*(observation - prediction);
				synchronized (this) {
				    this.train_rmse += squaredError;
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
     	   params.initParameters(this.datasetDesc);
		}
		// TODO Auto-generated method stub
		synchronized (this) {
			this.train_rmse = 0;
		}
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
	    
	    if(this.iterationNum > this.params.burnInPeriod){
	        //Save the parameters.
	        this.params.serialize(this.outputLoc, this.params.getId() + "_iter_" + this.iterationNum);
	    }
	    
	    this.train_rmse = Math.sqrt(this.train_rmse / (1.0 * ctx.getNumEdges()));
        this.logger.info("Current Sample's Train RMSE: " + this.train_rmse);
	    
		//Sample hyperparameters.
		sample_U();
		sample_V();
		
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
    public boolean hasConverged(GraphChiContext ctx) {
        return this.iterationNum == this.params.maxIterations;
    }
		
	protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Integer, PMFRatingEdge>(graphName, numShards, null, 
        		new EdgeProcessor<PMFRatingEdge>() {
		            public PMFRatingEdge receiveEdge(int from, int to, String token) {
		                return (token == null ? new PMFRatingEdge() : new PMFRatingEdge(Float.parseFloat(token),0.0f,0));
		            }
        	}, 
        	new IntConverter(), new PMFRatingEdgeConvertor());
    }

    @Override
    public ModelParameters getParams() {
        return this.params;
    }

    @Override
    public DataSetDescription getDataSetDescription() {
        return this.datasetDesc;
    }

    @Override
    public int getEstimatedMemoryUsage() {
        return this.params.getEstimatedMemoryUsage(this.datasetDesc);
    }

}

class PMFRatingEdge extends RatingEdge {
	float aggPred;
	int count;
	
	public PMFRatingEdge() {
		this.observation = 0;
		this.aggPred = 0;
		this.count = 0;
	}
	
	public PMFRatingEdge(float observation, float aggPred, int count) {
		this.observation = observation;
		this.aggPred = aggPred;
		this.count = count;
	}
}


class PMFRatingEdgeConvertor implements  BytesToValueConverter<PMFRatingEdge> {
    public int sizeOf() {
        return 12;
    }
    
    public PMFRatingEdge getValue(byte[] array) {
    	PMFRatingEdge res = null;
    	
    	ByteBuffer buf = ByteBuffer.wrap(array);
    	float obs = buf.getFloat(0);
    	float aggPred = buf.getFloat(4);
    	int count = buf.getInt(8);
    	
    	res = new PMFRatingEdge(obs, aggPred, count);
    	return res;
    }
    
    public void setValue(byte[] array, PMFRatingEdge val) {
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