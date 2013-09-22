package edu.cmu.graphchi.toolkits.collaborative_filtering;

import java.util.List;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.BlockRealMatrix;
import org.apache.commons.math.linear.CholeskyDecompositionImpl;
import org.apache.commons.math.linear.LUDecompositionImpl;
import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;

import scala.reflect.generic.Trees.ArrayValue;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.VertexInterval;

public class PMF implements GraphChiProgram<VertexDataType, EdgeDataType> {

	PMFProblemSetup setup;
	
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
	double nu0_U; //degrees of freedom of Gaussion Wishart hyperprior
	double beta0_U; //multiplicative factor to precision matrix in Gaussian-Wishart distribution.
	RealMatrix W0_U; //The scale matrix of Gaussian Wishart hyperprior.
	RealMatrix invW0_U; //Inverse of scale matrix of Gaussian Wishart hyperprior.
	RealVector mu0_U;	//mean hyperprior of the mean of latent factor vectors.
	RealMatrix lambda_U; // The precision matrix (inverse covariance matrix) for V (user's latent factors)
	RealVector mu_U; //The mean vector for U (user's latent factors)
	
	//For V (items)
	double nu0_V; //degrees of freedom of Gaussion Wishart hyperprior
	double beta0_V; //multiplicative factor to precision matrix in Gaussian-Wishart distribution.
	RealMatrix W0_V; //The scale matrix of Gaussian Wishart hyperprior.
	RealMatrix invW0_V; //Inverse of scale matrix of Gaussian Wishart hyperprior.
	RealVector mu0_V;	//mean hyperprior of the mean of latent factor vectors.
	RealMatrix lambda_V; // The precision matrix (inverse covariance matrix) for V (item's latent factors)
	RealVector mu_V; //The mean vector for V (item's latent factors)
	List<VertexDataType> latent_factors; //list of latent factors (U_i and V_j vectors for different U and V)
	
	//Other variables used while updation of different parameters.
	int N;	//Number of U_i's (users)	
	RealVector sumU;	//SUM U_i
	RealMatrix sumUUT;	//SUM U_i*U_i'
	
	int M;	//Number of U_i's (users)	
	RealVector sumV;	//SUM U_i
	RealMatrix sumVVT;	//SUM U_i*U_i'
	
	//Root Mean Squared Error
	double rmse;
	
	public void init_parameters() {
		this.alpha = 2.0;
		
		//Inititalize hyperparameters for U
		this.nu0_U = this.setup.D; //degrees of freedom equal to latent factors
		this.beta0_U = 2.0;
		this.W0_U = eye(this.setup.D);
		this.invW0_U = eye(this.setup.D);
		this.mu0_U = new ArrayRealVector();
		
		//Inititalize hyperparameters for V
		this.nu0_V = this.setup.D; //degrees of freedom equal to latent factors
		this.beta0_V = 2.0;
		this.W0_V = eye(this.setup.D);
		this.invW0_V = eye(this.setup.D);
		this.mu0_V = new ArrayRealVector();
		
		//Initializing precision for U and V to identity.
		this.lambda_U = eye(this.setup.D);
		this.lambda_V = eye(this.setup.D);
		
		//Initializing mean for U and V to 0.
		this.mu_U = new ArrayRealVector();
		this.mu_V = new ArrayRealVector();
		
	}
	
	//TODO: Move this to utils.
	private RealMatrix eye(int dim) {
		RealMatrix m = new BlockRealMatrix(dim, dim);
		for(int i = 0; i < dim; i++) {
			m.setEntry(i, i, 1);
		}
		return m;
	}
	
	/**
	 * meanU = (SUM U_i)/N
	 * meanS = (SUM (U_i*U_i')/N)
	 * mu0 = (beta0*mu0 + meanU)/(beta0 + N)
	 * beta0 = beta0 + N
	 * nu0 = nu0 + N
	 * W0 = inv( inv(W0) + N*meanS + (beta0*N/(beta0 + N))(mu0 - meanU)*(mu0 - meanU)'
	 */
	public void sample_U() {
		//Note, sumU and sumUUT are update in the update function itself. Hence, when
		//sample_U is called after the 1 iteration of all vertices, we already have the sum.
		
		//meanU = (SUM U_i)/N
		RealVector meanU = this.sumU.mapMultiply(1.0/this.N);
		//meanS = (SUM (U_i*U_i')/N)
		RealMatrix meanS = this.sumUUT.scalarMultiply(1.0/this.N);
		
		//mu0 = (beta0*mu0 + meanU)/(beta0 + N)
		RealVector mu0_ = (this.mu0_U.mapMultiply(this.beta0_U).add(meanU)).mapDivide((this.beta0_U + this.N));
		
		double beta0_ = this.beta0_U + this.N;
		double nu0_ = this.nu0_U + this.N;
		
		//W0 = inv( inv(W0) + N*meanS + (beta0*N/(beta0 + N))(mu0 - meanU)*(mu0 - meanU)'
		RealVector tmp = this.mu0_U.subtract(meanU);
		RealMatrix mu0_d_meanU_T = tmp.outerProduct(tmp); 
		mu0_d_meanU_T = mu0_d_meanU_T.scalarMultiply((this.beta0_U*N/(this.beta0_U + N)));
		RealMatrix invW0_ = this.invW0_U.add(this.sumUUT).add(mu0_d_meanU_T);
		
		//Update all the values.
		this.mu0_U = mu0_;
		this.beta0_U = beta0_;
		this.nu0_U = nu0_;
		this.invW0_U = invW0_;
		this.W0_U = (new LUDecompositionImpl(invW0_)).getSolver().getInverse();
		
		//Reset the sum of latent factors.
		this.sumU.mapMultiply(0);
		this.sumUUT.scalarMultiply(0);
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
		RealVector meanV = this.sumV.mapMultiply(1.0/this.M);
		//meanS = (SUM (V_j*V_j')/N)
		RealMatrix meanS = this.sumVVT.scalarMultiply(1.0/this.M);
		
		//mu0 = (beta0*mu0 + meanV)/(beta0 + N)
		RealVector mu0_ = (this.mu0_V.mapMultiply(this.beta0_V).add(meanV)).mapDivide((this.beta0_V + this.M));
		
		double beta0_ = this.beta0_V + this.M;
		double nu0_ = this.nu0_V + this.M;
		
		//W0 = inv( inv(W0) + N*meanS + (beta0*N/(beta0 + N))(mu0 - meanU)*(mu0 - meanU)'
		RealVector tmp = this.mu0_V.subtract(meanV);
		RealMatrix mu0_d_meanV_T = tmp.outerProduct(tmp); 
		mu0_d_meanV_T = mu0_d_meanV_T.scalarMultiply((this.beta0_V*N/(this.beta0_V + N)));
		RealMatrix invW0_ = this.invW0_V.add(this.sumVVT).add(mu0_d_meanV_T);
		
		//Update all the values.
		this.mu0_V = mu0_;
		this.beta0_V = beta0_;
		this.nu0_V = nu0_;
		this.invW0_V = invW0_;
		this.W0_V = (new LUDecompositionImpl(invW0_)).getSolver().getInverse();
		
		//Reset the sum of latent factors.
		this.sumV.mapMultiply(0);
		this.sumVVT.scalarMultiply(0);
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
	public void update(ChiVertex<VertexDataType, EdgeDataType> vertex, GraphChiContext context) {
		boolean isUser = vertex.numOutEdges() > 0;
		VertexDataType vData = this.latent_factors.get(vertex.getId());
		
		//Will be updated to store SUM(V_j*R_ij)
		RealVector Xty = new ArrayRealVector(this.setup.D);
		//Will be updated to store SUM(V_j*V_j')
		RealMatrix XtX = new BlockRealMatrix(this.setup.D, this.setup.D);
		
		//Gather data to update the mean and the covariance for the hidden features.	
		for(int i = 0; i < vertex.numEdges(); i++) {
			EdgeDataType edge = vertex.edge(i).getValue();
			double observation = edge.weight;
			
			int nbrId = vertex.edge(i).getVertexId();
			VertexDataType nbrVertex = this.latent_factors.get(nbrId);
			
			//Add V_j*R_ij for this observation.
			Xty = Xty.add(nbrVertex.pVec.mapMultiply(observation));
			
			//Add V_j*V_j' for this 
			XtX = XtX.add(nbrVertex.pVec.outerProduct(nbrVertex.pVec));
			
		}
		
		RealMatrix lambda_prior = isUser ? this.lambda_U : this.lambda_V;
		RealVector mu_prior = isUser ? this.mu_U : this.mu_V;
		
		RealMatrix precision = lambda_prior.add(XtX.scalarMultiply(this.alpha)); 
		RealMatrix covariance = (new LUDecompositionImpl(precision).getSolver().getInverse());
		
		RealVector tmp = lambda_prior.operate(mu_prior);
		RealVector mean = covariance.operate(Xty.mapMultiply(this.alpha).add(tmp));
		
		//We have the covariance and mean. We can grab a sample from this multivariate
		//normal distribution according to: 
		// http://en.wikipedia.org/wiki/Multivariate_normal_distribution#Drawing_values_from_the_distribution
		//Javadoc:
		MultivariateNormalDistribution dist = new MultivariateNormalDistribution(mean.toArray(), covariance.getData());
		vData.pVec = new ArrayRealVector(dist.sample());
		
		//Aggregate the sample and compute rmse if greater than burn_in period
		boolean burnedIn = context.getIteration() > this.setup.burn_in_period; 
		if(burnedIn) {
			vData.aggVec.add(vData.pVec);
			vData.count++;
		} 
		
		synchronized (this) {
			this.rmse += computeRMSE(vertex, burnedIn);
		}
		
		//Update Sum of U_i / V_j
		if(isUser) {
			this.sumU = this.sumU.add(vData.pVec);
			this.sumUUT = this.sumUUT.add(vData.pVec.outerProduct(vData.pVec));
		} else {
			this.sumV = this.sumV.add(vData.pVec);
			this.sumVVT = this.sumVVT.add(vData.pVec.outerProduct(vData.pVec));
		}
	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		if (ctx.getIteration() == 0) {
     	   init_parameters();
		}
		// TODO Auto-generated method stub
		synchronized (this) {
			this.rmse = 0;
		}
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		// TODO Auto-generated method stub
		//Sample hyperparameters.
		sample_U();
		sample_V();
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
	
	
	public double computeRMSE(ChiVertex<VertexDataType, EdgeDataType> v, boolean burnedIn) {
		VertexDataType vData = this.latent_factors.get(v.getId());
		double rmse = 0;
		for(int i = 0; i <  v.numOutEdges(); i++) {
			VertexDataType nbr = this.latent_factors.get(v.edge(i).getVertexId());
			
			double prediction;
				if(burnedIn) {
					RealVector tmp1 = vData.aggVec.mapMultiply(1.0/vData.count);
					RealVector tmp2 = nbr.aggVec.mapMultiply(1.0/nbr.count);
					prediction = tmp1.dotProduct(tmp2);
				} else {
					prediction = vData.pVec.dotProduct(nbr.pVec);
				}
			double observation = v.edge(i).getValue().weight;
			rmse += (observation - prediction)*(observation - prediction);
		}
		
		return rmse;
	}
	
	static class PMFProblemSetup extends ProblemSetup {
		//Parameters - hyperpriors
		int burn_in_period;
		
		public PMFProblemSetup(String[] args) {
			super(args);
			
			//TODO: Allow burn in period to be passed as a command line argument.
			this.burn_in_period = 5;
		}
	}

}

class VertexDataType {
	RealVector pVec;
	RealVector aggVec;
	int count;
	
	public VertexDataType(int D) {
		this.pVec = new ArrayRealVector(D);
		this.count = 0;
		for(int i = 0; i < D; i++) {
			this.pVec.setEntry(i, Math.random());
			this.aggVec.setEntry(i, 0);
		}
	}
}

class EdgeDataType {
	double avgPred;
	double weight;
	
	public EdgeDataType() {
		this.weight = 0;
		this.avgPred = 0;
	}
	
	public EdgeDataType(double weight) {
		this.weight = weight;
	}
}
