package edu.cmu.graphchi.toolkits.collaborative_filtering;


import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.VertexInterval;

/**
 * This is the implementation of Factorization Machines using the MCMC method
 * based on the algorithm given in the following paper:
 * "Steffen Rendle (2012): Factorization Machines with libFM, in ACM Trans. Intell. Syst. Technol., 3(3), May"
 * 
 * @author mayank
 *
 */

class ModelParams {
	/**
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
	 */
	
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
	
	
	//Various things
	
	//Various things required to update parameters
	//TODO: The updates to this should be guarded by locks? A lock for each update might be too expensive?
	double[] sum_w_h_theta_2;
	double[] sum_w_h_theta_error;
	double[][] sum_v_h_theta_2;
	double[][] sum_v_h_theta_error;
	
	double sum_error;
	double sum_error_2;
	
	//Various sums required for updating hyper parameters.
	double sum_param_mean_2;
	double sum_param;
}


public class LibFM_MCMC  implements GraphChiProgram<VertexDataType, EdgeDataType>{
	private ModelParams params;
	private int num_users;
	private int num_items;
	private int num_other_features;
	private int num_features;
	
	private int k;
	
	public LibFM_MCMC(int num_users, int num_items, int num_other_features) {
		this.num_users = num_users;
		this.num_items = num_items;
		this.num_other_features = num_other_features;
	}
	
	//Sample.
	
	public void sample_all() {
		//Sample Hyper-parameters.
		draw_alpha(this.num_features);
		
		draw_w_lambda(this.num_features);
		
		draw_w_mu(this.num_features);
		
		draw_v_lambda(this.num_features);
		
		draw_v_mu(this.num_features);
		
		//Sample Parameters.
		draw_parameters();
		
	}
	
	//Sampling the parameters
	private void draw_parameters() {
		//This function is based on the equations 28, 29 and 30 in the paper: 
		//Factorization Machines with LibFM (Stephen Rendle, 2012)
		
		//theta ~ N(mu, sigma)
		//sigma^2 = (alpha*(SUM_i_n h_theta_i_2) + lambda_theta
		//mu = sigma^2(alpha*theta*(SUM_i_n h_theta_i_2) + alpha*(SUM_i_n h_theta_i*e_i) + mu*lambda)
		
		//Here h_theta differs based on what the parameter is: (Equation 7 in LibFM paper)  
		//theta = w0    ===>  h_theta = 1
		//theta = w_l   ===>  h_theta = x_l
		//theta = v_l,f ===>  h_theta = x_l*SUM(v_j,f*x_j) where j != l
		
		params.w_0 = draw_gaussian_sample(params.w_0, params.w0_mu, params.w0_lambda, this.num_features, params.sum_error);
		
		//Update w_0, 0 way parameter.		
		for(int j = 0; j < num_features; j++) {
			params.w[j] = draw_gaussian_sample(params.w[j], params.w_mu, params.w_lambda, params.sum_w_h_theta_2[j], 
					params.sum_w_h_theta_error[j]);
			for(int f = 0; f < k; f++) {
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
		for(int f = 0; f < k; f++) {
			double gaussian_mean = 1.0/(countGroup + params.gamma_0);
			gaussian_mean *= (params.sum_param + params.gamma_0*params.mu_0);
			double gaussian_variance = 1.0/( (countGroup + params.gamma_0)*params.w_lambda);
	
			NormalDistribution dist = new NormalDistribution(gaussian_mean, Math.sqrt(gaussian_variance));
			params.v_mu[f] = dist.sample();
		}
	}
	
	private void draw_v_lambda(int countGroup) {		
		for(int f = 0; f < k; f++) {
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

	private void draw_alpha(int numObs) {
		double alpha_posterior = (params.alpha_0 + numObs)/2.0;
		double beta_posterior = (params.sum_error_2 + params.beta_0)/2.0;
		
		GammaDistribution dist = new GammaDistribution(alpha_posterior, 1.0/beta_posterior);
		params.alpha = dist.sample();
	}

	@Override
	public void update(ChiVertex<VertexDataType, EdgeDataType> vertex, GraphChiContext context) {

	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		// TODO Auto-generated method stub
		
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
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

class VertexDataType {
	int id;		//feature index.
	
	//These 2 lists represent the sparse vector for user/item features.
	int[] features;		//feature indices. ()
	float[] feature_values;		//feature values.
}

class EdgeDataType {
	float observation;
	
}