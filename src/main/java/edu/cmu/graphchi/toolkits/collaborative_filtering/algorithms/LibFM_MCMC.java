/*package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import gov.sandia.cognition.math.matrix.VectorEntry;
import gov.sandia.cognition.math.matrix.mtj.SparseMatrix;
import gov.sandia.cognition.math.matrix.mtj.SparseMatrixFactoryMTJ;
import gov.sandia.cognition.math.matrix.mtj.SparseRowMatrix;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;
import gov.sandia.cognition.math.matrix.mtj.SparseVectorFactoryMTJ;

*//**
 * This is the implementation of Factorization Machines using the MCMC method
 * based on the algorithm given in the following paper:
 * "Steffen Rendle (2012): Factorization Machines with libFM, in ACM Trans. Intell. Syst. Technol., 3(3), May"
 * 
 * @author mayank
 *
 *//*

class LibFM_MCMCParams extends ModelParameters {
	*//**
	 * Graphical Model for Bayesian Factorization machine.
	 * 
	 *      	    mu_0, gamma_0-----\/-----alpha_lambda, beta_lamda         [hyperpriors]
	 * 				   |			  /\                  |
	 * 				   |       ------/	\-----------      |
	 * 				   |	   |                   |      |
	 * 				   v       v                   v      v
	 * 				(w_mu)<---(w_lambda)          (v_mu)<---(v_lambda)     [hyper parameters]
	 * 					\      |                       \      /
	 * 					 \	  / 	                    \    /  
	 *                    \  /                           \  /
	 *      [1-way]       (w_j)                        (v_j,f)   [2 way]      [parameters]
	 *                         \                       /
	 *                          \       (x_i,j)       /                       [Observed Features]                     
	 *                    		 \         |         /
	 *                    		  -----\   v        /
 	 *                 ----------------->( y_i )<---  	                      [Observed value]
 	 *  [0-way]  (w_0)/                     ^      
	 *              ^                       |
	 *              |                       |
[hyperpriors]  w0_mu, w0_lambda             (alpha)
                                            ^    
								            |
								     alpha_0, beta_0   
	 *//*
	
	//Hyperpriors
	double mu_0, gamma_0; 
	double alpha_lamda, beta_lambda;
	double w0_mu, w0_lambda;
	double alpha_0, beta_0;
	
	//Hyper parameters.
	double w_mu, w_lambda;
	double[] v_mu, v_lambda; //length = Number of features 
	double alpha; 
	
	//Parameters. (Should this be stored in memory or should it be stored in the vertex / edge?
	double w_0;		//0-way interactions
	double[] w;		//1-way interactions
	double[][] v;	//2-way interactions

	int D;			//Number of latent features 
	
	//Various things required to update parameters
	//TODO: The updates to this should be guarded by locks? A lock for each update might be too expensive?
	volatile double[] sum_w_h_theta_2;
	volatile double[] sum_w_h_theta_error;
	volatile double[][] sum_v_h_theta_2;
	volatile double[][] sum_v_h_theta_error;
	
	volatile double sum_error;
	volatile double sum_error_2;
	
	//Various sums required for updating hyper parameters.
	double sum_param_mean_2;
	double sum_param;

	int num_users;
	int num_items;
	int num_user_features;
	int num_item_features;
	int num_edge_features;
	int num_features;
	
	public LibFM_MCMCParams(String id, String json) {
		super(id, json);
		
		setDefaults();
		
		parseJsonParams(json);
	}
	
	private void parseJsonParams(String json) {

	}

	private void setDefaults() {
		this.D = 10;
		
		//Hyperpriors - To be set by user
		this.mu_0 = 0; 
		this.gamma_0 = 1.0;
		
		this.alpha_lamda = 1;
		this.beta_lambda = 1;
		
		this.alpha_0 = 1.0;
		this.beta_0 = 1.0;
		
		this.w0_mu = 0;
		this.w0_lambda = 1;
		
		//TODO: Read from info file?
		this.num_users = 943;
		this.num_items = 1682;
		this.num_user_features = 38;
		this.num_item_features = 20;
		this.num_edge_features = 0;
		this.num_features = this.num_users + this.num_items + this.num_user_features + 
			this.num_item_features + this.num_edge_features;
	}
	
	public void initParameters() {
		this.w_mu = 0;
		this.w_lambda = 0;
		this.v_mu = new double[this.D];
		this.v_lambda = new double[this.D];
		this.alpha = 0;
		
		//Parameters. (Should this be stored in memory or should it be stored in the vertex / edge?
		this.w_0 = 0;									//0-way interactions
		this.w = new double[this.num_features];			//1-way interactions
		this.v = new double[this.num_features][this.D];	//2-way interactions
		
		this.sum_w_h_theta_2 = new double[this.num_features];
		this.sum_w_h_theta_error = new double[this.num_features];
		this.sum_v_h_theta_2 = new double[this.num_features][this.D];
		this.sum_v_h_theta_error = new double[this.num_features][this.D];
		
		this.sum_error = 0;
		this.sum_error_2 = 0;
		
		//Various sums required for updating hyper parameters.
		this.sum_param_mean_2 = 0;
		this.sum_param = 0;
		
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


public class LibFM_MCMC  implements GraphChiProgram<Integer, LibFMEdge> {
	private ProblemSetup setup;
	private LibFM_MCMCParams params;
	
	//Contains data about user and item features. Currently this is held in memory.
	SparseRowMatrix features;

	protected Logger logger = ChiLogger.getLogger("LibFM_MCMC");
	
	//Train RMSE
	double train_rmse;
	
	//Contains aggregate of all the samples generated in the iteration for testing.
	TestDataSamples testSamples;
	
	public LibFM_MCMC(ProblemSetup setup, ModelParameters par) {
		this.params = (LibFM_MCMCParams)par;
		this.setup = setup;
	}
	
	//Sample.
	public void sample_all(GraphChiContext context) {
		//Sample Hyper-parameters.
		draw_alpha(context.getNumEdges());
		
		draw_w_lambda(params.num_features);
		
		draw_w_mu(params.num_features);
		
		draw_v_lambda(params.num_features);
		
		draw_v_mu(params.num_features);
		
		//Sample Parameters.
		draw_parameters();
		
	}
	
	//Sampling the parameters
	private void draw_parameters() {
		//This function is based on the equations 28, 29 and 30 in the paper: 
		//Factorization Machines with LibFM (Stephen Rendle, 2012)
		
		 
		 * theta ~ N(mu, sigma)
		 * sigma^2 = (alpha*(SUM_i_n h_theta_i_2) + lambda_theta
		 * mu = sigma^2(alpha*theta*(SUM_i_n h_theta_i_2) + alpha*(SUM_i_n h_theta_i*e_i) + mu*lambda)
		 * //Here h_theta differs based on what the parameter is: (Equation 7 in LibFM paper)  
		 * theta = w0    ===>  h_theta = 1
		 * theta = w_l   ===>  h_theta = x_l
		 * theta = v_l,f ===>  h_theta = x_l*SUM(v_j,f*x_j) where j != l
		 
		
		//Update w_0, 0 way parameter.		
		params.w_0 = draw_gaussian_sample(params.w_0, params.w0_mu, params.w0_lambda, params.num_features, params.sum_error);
		
		for(int j = 0; j < params.num_features; j++) {
			//Update w_i -> 1 way parameter 
			params.w[j] = draw_gaussian_sample(params.w[j], params.w_mu, params.w_lambda, params.sum_w_h_theta_2[j], 
					params.sum_w_h_theta_error[j]);
			//Update 2-way parameters. 
			for(int f = 0; f < params.D; f++) {
				params.v[j][f] = draw_gaussian_sample(params.v[j][f], params.v_mu[f], params.v_lambda[f], 
						params.sum_v_h_theta_2[j][f], params.sum_v_h_theta_error[j][f]);
			}
		}
	}
	
	private double draw_gaussian_sample(double theta, double prior_mean, double prior_lambda, double sum_h_theta_2, double sum_h_theta_error) {
		double gaussian_variance = 1.0/(params.alpha*sum_h_theta_2 + prior_lambda);
		double gaussian_mean = gaussian_variance*(params.alpha*theta*sum_h_theta_2 + 
			params.alpha*sum_h_theta_error + prior_mean*prior_lambda);
		
		NormalDistribution dist = new NormalDistribution(gaussian_mean, Math.sqrt(gaussian_variance));
		double new_theta_sample = dist.sample();
		return new_theta_sample;
	}

	//Sampling hyperparameters. 
	//TODO: Currently parameter groups are not supported. Need to support it.
	private void draw_v_mu(int countGroup) {
		for(int f = 0; f < params.D; f++) {
			double gaussian_mean = 1.0/(countGroup + params.gamma_0);
			gaussian_mean *= (params.sum_param + params.gamma_0*params.mu_0);
			double gaussian_variance = 1.0/( (countGroup + params.gamma_0)*params.w_lambda);
	
			NormalDistribution dist = new NormalDistribution(gaussian_mean, Math.sqrt(gaussian_variance));
			params.v_mu[f] = dist.sample();
		}
	}
	
	private void draw_v_lambda(int countGroup) {		
		for(int f = 0; f < params.D; f++) {
			double alpha_posterior = (params.alpha_lamda + countGroup + 1)/2.0;
			double beta_posterior = (params.sum_param_mean_2 + params.gamma_0*(params.v_mu[f] - params.mu_0) 
					+ params.beta_lambda)/2.0;
		
			GammaDistribution dist = new GammaDistribution(alpha_posterior, 1.0/beta_posterior);
			params.v_lambda[f] = dist.sample();
		}
	}

	private void draw_w_mu(int countGroup) {
		double gaussian_mean = 1.0/(countGroup + params.gamma_0);
		gaussian_mean *= (params.sum_param + params.gamma_0*params.mu_0);
		double gaussian_variance = 1.0/( (countGroup + params.gamma_0)*params.w_lambda);

		NormalDistribution dist = new NormalDistribution(gaussian_mean, Math.sqrt(gaussian_variance));
		params.w_mu = dist.sample();
	}

	private void draw_w_lambda(int countGroup) {
		double alpha_posterior = (params.alpha_lamda + countGroup + 1)/2.0;
		double beta_posterior = (params.sum_param_mean_2 + params.gamma_0*(params.w_mu - params.mu_0) 
			+ params.beta_lambda)/2.0;
		
		GammaDistribution dist = new GammaDistribution(alpha_posterior, 1.0/beta_posterior);
		params.w_lambda = dist.sample();
	}

	private void draw_alpha(long numObs) {
		double alpha_posterior = (params.alpha_0 + numObs)/2.0;
		double beta_posterior = (params.sum_error_2 + params.beta_0)/2.0;
		
		GammaDistribution dist = new GammaDistribution(alpha_posterior, 1.0/beta_posterior);
		params.alpha = dist.sample();
	}
	
	public double predict(SparseVector row) {
		//y = w0 +
		double estVal = this.params.w_0;
		
		Iterator<VectorEntry> it = row.iterator();
		while(it.hasNext()) {
			VectorEntry xi = it.next();
			double wi = this.params.w[xi.getIndex()];
			double[] vi = this.params.v[xi.getIndex()];  
			
			// wi*xi +
			estVal += wi*xi.getValue();
			
			Iterator<VectorEntry> it2 = row.iterator();
			while(it2.hasNext()) {
				VectorEntry xj = it2.next();
				double[] vj = this.params.v[xj.getIndex()];
				double dotProd = 0;
				for(int f = 0; f < this.params.D; f++) {
					dotProd += vi[f]*vj[f];
				}
				
				// <vi, vj>*xi*xj +
				estVal += dotProd*xi.getValue()*xj.getValue();
			}
		}
		
		estVal = Math.min(estVal, this.setup.minval);
		estVal = Math.max(estVal, this.setup.maxval);
		
		return estVal;
	}

	private SparseVector createAllFeatureVec(int user, int item, SparseVector userFeatures, 
			SparseVector itemFeatures) {
		//Construct a row of the design matrix.
		SparseVector allFeatures = (new SparseVectorFactoryMTJ()).createVector(params.num_features);
		
		//Set feature representing an user.
		allFeatures.setElement(user, 1);
		//Set feature representing an item.
		allFeatures.setElement(item, 1);
		
		//Set features representing user attributes.
		Iterator<VectorEntry> it = userFeatures.iterator();
		while(it.hasNext()) {
			VectorEntry feature = it.next();
			allFeatures.setElement((int)(feature.getIndex()), feature.getValue());
		}
		
		//Set features representing item attributes.
		it = itemFeatures.iterator();
		while(it.hasNext()) {
			VectorEntry feature = it.next();
			allFeatures.setElement((int)(feature.getIndex()), feature.getValue());
		}
		
		return allFeatures;
	}
	
	@Override
	public void update(ChiVertex<Integer, LibFMEdge> vertex, GraphChiContext context) {
		if(vertex.numOutEdges() > 0) {
			//User vertex
			
			//Update user feature aggregates.
			SparseVector userFeatures = this.features.getRow(vertex.getId());
			
			for(int e = 0; e < vertex.numOutEdges(); e++) {
				SparseVector itemFeatures = this.features.getRow(vertex.getOutEdgeId(e));
				LibFMEdge edge = vertex.edge(e).getValue();

				SparseVector allFeatures = createAllFeatureVec(vertex.getId(), vertex.getOutEdgeId(e) , 
						userFeatures, itemFeatures);
				
				//TODO: Set features representing edge attributes (like time stamp)
				double estVal = predict(allFeatures);
				double err = edge.observation - estVal; 
				
				double[] sum_all_v_2 = new double[params.D];
				double[] sum_all_v_error = new double[params.D];
				
				Iterator<VectorEntry> it = allFeatures.iterator();
				while(it.hasNext()) {
					VectorEntry vec = it.next();
					int j = vec.getIndex();
					for(int f = 0; f < params.D; f++) {
						sum_all_v_2[f] += params.v[j][f]*params.v[j][f];
						sum_all_v_error[f] += params.v[j][f]*err;
					}
				}
				
				it = allFeatures.iterator();
				while(it.hasNext()) {
					VectorEntry vec = it.next();
					int j = vec.getIndex();
					
					//Update 1 way weights
					//TODO: Atomic?
					params.sum_w_h_theta_2[j] += vec.getValue()*vec.getValue(); 
					params.sum_w_h_theta_error[j] += vec.getValue()*err;
					
					//Update 2 way weigts
					for(int f = 0; f < params.D; f++) {
						params.sum_v_h_theta_2[j][f] += (sum_all_v_2[f] - params.v[j][f]*params.v[j][f]);
						params.sum_v_h_theta_error[j][f] += (sum_all_v_error[f] - params.v[j][f]*err);
					}
					
				}
				
				//TODO: Make Atomic
				params.sum_error += err;
				params.sum_error_2 += err*err;
				
				//Compute RMSE
				edge.aggPred += estVal;
				edge.count++;
				double globalErr = edge.observation - edge.aggPred/edge.count;
				this.train_rmse += globalErr*globalErr;
						
			}
				
		} else {
			//Item vertex
		}
	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		if(ctx.getIteration() == 0) {
			//On First iteration
			this.params.initParameters();
			
			this.features = (new SparseMatrixFactoryMTJ()).createMatrix(params.num_users + params.num_items + 1, 
					params.num_users + params.num_items + params.num_user_features + params.num_item_features + 1);
			
			//Read User features
			//readFeatures(getUserBase(), getUserFeatureBase(), this.setup.userFeatures, true);
			
			//Read Item features
			//readFeatures(getItemBase(), getItemFeatureBase(), this.setup.itemFeatures, false);
		}
		
		this.train_rmse = 0;
		for(int j = 0; j < this.params.num_features; j++) {
			this.params.sum_w_h_theta_2[j] = 0;
			this.params.sum_w_h_theta_error[j] = 0;
			for(int f = 0; f < this.params.D; f++) {
				this.params.sum_v_h_theta_2[j][f] = 0;
				this.params.sum_v_h_theta_error[j][f] = 0;
			}
		}
		
		this.params.sum_error = 0;
		this.params.sum_error_2 = 0;
		this.params.sum_param = 0;
		this.params.sum_param_mean_2 = 0;
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		//TODO: Persist this set of parameters. In an MCMC method, many different sets of parameters are 
		// generated and from each set, samples are drawn.
		this.params.serialize(null);
		
		sample_all(ctx);
		
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
	
	private int getUserBase(){
		return 0;
	}
	
	private int getItemBase() {
		return getUserBase() + this.params.num_users;
	}
	
	private int getUserFeatureBase() {
		return getItemBase() + this.params.num_items;
	}
	
	
	private int getItemFeatureBase() {
		return getUserFeatureBase() + this.params.num_user_features;
	}
	
	private int getEdgeFeaturesBase() {
		return getItemFeatureBase() + this.params.num_item_features;
	}
	
	*//**
	 * 
	 * @param vertexBase
	 * @param featureBase
	 * @param fileName
	 * @return
	 *//*
	private void readFeatures(int vertexBase, int featureBase, String fileName, boolean isUser) {
		if(fileName == null)
			return;
		BufferedReader br = null;
		
		try {
			br = new BufferedReader(new FileReader(new File(fileName)));
			String line;
			
			while( (line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				int user = vertexBase + Integer.parseInt(tokens[0]);
				for(int i = 1; i < tokens.length; i++) {
					int featureId = featureBase + Integer.parseInt(tokens[i].split(":")[0]);
					double featureVal = Double.parseDouble(tokens[i].split(":")[1]);
					features.setElement(user, featureId, featureVal);
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(br != null)
					br.close();
			} catch(IOException e2) {
				e2.printStackTrace();
			}
		}
	}
	
	private double test() {
		if(this.setup.test == null)
			return 0;
		
		
		BufferedReader br = null;
		LibFMEdgeProcessor edgeProc = new LibFMEdgeProcessor();
		double rmse = 0;
		int num_test_cases = 0;
		
		try {
			br = new BufferedReader(new FileReader(new File(this.setup.test)));
			String line;
			
			while( (line = br.readLine()) != null) {
				num_test_cases += 1;
				//TODO: There should be a EdgeDataProcessor - the same as that used by Sharder program to read
				//in the edges (which represent observation between the user and item.
				String[] tokens = line.split("\t", 3);
				int from = Integer.parseInt(tokens[0]);
				int to = Integer.parseInt(tokens[1]);
				LibFMEdge e = edgeProc.receiveEdge(from, to, tokens[2]);
				
				SparseVector userFeatures = this.features.getRow(from);
				SparseVector itemFeatures = this.features.getRow(to);
				
				SparseVector allFeatures = createAllFeatureVec(from, to, userFeatures, itemFeatures);
				double estSample = this.predict(allFeatures);
				
				rmse += (e.observation -  estSample)*(e.observation -  estSample); 
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(br != null)
					br.close();
			} catch(IOException e2) {
				e2.printStackTrace();
			}
		}
		
		return Math.sqrt(rmse / (1.0 * num_test_cases));
	}

	protected static FastSharder createSharder(String graphName, int numShards, int num_edge_features) throws IOException {
        return new FastSharder<Integer, LibFMEdge>(graphName, numShards, null, 
        		new LibFMEdgeProcessor(), 
        	new IntConverter(), new LibFMEdgeConvertor(num_edge_features));
    }
	
	public static void main(String[] args) throws Exception {
    	ProblemSetup problemSetup = new ProblemSetup(args);

    	//TODO: Edge Features.
    	FastSharder<Integer, LibFMEdge> sharder = createSharder(problemSetup.training, problemSetup.nShards, 0);
        IO.convertMatrixMarket(problemSetup, sharder);
        
        LibFM_MCMCParams params = new LibFM_MCMCParams(problemSetup.getRunId("LibFM_MCMC"), problemSetup.paramJson);
        GraphChiProgram<Integer, LibFMEdge> libfm = new LibFM_MCMC(problemSetup, params);
        
        // Run GraphChi 
        GraphChiEngine<Integer, LibFMEdge> engine = new GraphChiEngine<Integer, LibFMEdge>(problemSetup.training, problemSetup.nShards);
        
        //TODO: Edge features.
        engine.setEdataConverter(new LibFMEdgeConvertor(0));
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        engine.setModifiesOutedges(false); // Important optimization
        engine.run(libfm, 15);
       
        params.serialize(problemSetup.outputLoc);
		
	}

}


class LibFMEdge {
	float observation;
	//TODO: support for edge features - a sparse vector representing the features of the edge

	//Sample aggregates for computing model error and 
	float aggPred;
	int count;
	
	public LibFMEdge() {
		this.observation = 0;
		this.aggPred = 0;
		this.count = 0;
	}
	
	public LibFMEdge(float obs, int num_edge_features) {
		this.observation = obs;
		this.aggPred = 0;
		this.count = 0;
	}
	
}

class LibFMEdgeConvertor implements  BytesToValueConverter<LibFMEdge> {
	int num_edge_features;
	
	public LibFMEdgeConvertor(int num_edge_features) {
		this.num_edge_features = num_edge_features;
	}
	
    public int sizeOf() {
        return 12 + this.num_edge_features*8;
    }
    
    public LibFMEdge getValue(byte[] array) {
    	LibFMEdge res = null;
    	
    	ByteBuffer buf = ByteBuffer.wrap(array);
    	float obs = buf.getFloat(0);
    	float aggPred = buf.getFloat(4);
    	int count = buf.getInt(8);
    	//TODO: Read edge features.
    	
    	res = new LibFMEdge(obs, this.num_edge_features);
    	res.aggPred = aggPred;
    	res.count = count;
    	
    	return res;
    }
    
    public void setValue(byte[] array, LibFMEdge val) {
    	  ByteBuffer buf = ByteBuffer.allocate(12);
    	  buf.putFloat(0, val.observation);
    	  buf.putFloat(4, val.aggPred);
    	  buf.putInt(8, val.count);
    	  //TODO: Write edge features.
    	  
    	  byte[] a = buf.array();
    	  
    	  for(int i = 0; i < a.length; i++) {
    		  array[i] = a[i];
    	  }
    }
}


class LibFMEdgeProcessor implements EdgeProcessor<LibFMEdge> {

	@Override
	public LibFMEdge receiveEdge(int from, int to, String token) {
		if(token == null) {
			return new LibFMEdge(0, 0);
		} else {
			String[] tokens = token.split("\t");
			LibFMEdge e = new LibFMEdge(Float.parseFloat(tokens[0]), 0);
			//TODO: Implement parsing other edge features like timestamp
			
			return e;
		}
	}
	
}

class TestDataSamples {
	String testFile;
	int samplesCount;
	
	//Contains sum of samples of predictions.
	double[] sumPredictions;
	
	public TestDataSamples(String testFile, int numTestCases) {
		
	}
}
*/