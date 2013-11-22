package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.util.Arrays;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.SerializationUtils;
import edu.cmu.graphchi.util.HugeDoubleMatrix;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;

/**
 * SVD++ algorithm with Stochastic Gradient Descent.
 * 
 * This code is based on GraphChi-Cpp's implementation of SVD++ by Danny Bickson (CMU) 
 * 
 * It is based on the algorithm given in the following paper:
 * Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative Filtering Model. 
 * ACM SIGKDD 2008.
 * http://dl.acm.org/citation.cfm?id=1401944 
 *long
 * <i>Note:</i>  in this case the vertex values are not used, but as GraphChi does
 * not currently support "no-vertex-values", integer-type is used as placeholder.
 *
 * @author Mayank Mohta, mmohta@andrew.cmu.edu, 2013
 */

class SVDPPParams extends ModelParameters {
	private static final long serialVersionUID = -363718674763291726L;
	//Model parameters to be computed
	
	// Number of iterations - Stopping condition. 
	//TODO: Note that may be we can have a better stopping condition based on change in training RMSE.  
	int maxIterations;
	
	//Computed in the first iteration of update functions currently.
	int numUsers;				//Number of users.
	int numItems;				//Number of items.
	double globalMean;			//global mean of all ratings.
	
	HugeDoubleMatrix latentFactors;	//latent factors for both users and items (numUsers+numItems) x D
	RealVector bias;					//Bias terms for both users and items. (numUsers+numItems) x 1
	HugeDoubleMatrix weights;		//(numUsers+numItems) x D  For items, this contains weights.  
									// For users it contains sum of weights of neighboring items.
	
	//meta parameters.
	int numFactors;	//Num features
	
	//Various regularization and step parameters
	double itemFactorStep;	//gamma2
	double itemFactorReg;	//lamda7
	double userFactorStep;	//gamma2
	double userFactorReg;	//lambda7
	double itemBiasReg;		// lamda6
	double userBiasReg;		// lambda6
	double itemBiasStep;	//gamma1
	double userBiasStep;	//gamma1
	double stepDec; 		//Amount by which step size should be reduced.
	
	public SVDPPParams(String id, Map<String, String> paramsMap) {
		super(id, paramsMap);
		setDefaults();
		
		parseParameters();
	}
	
	private void setDefaults() {
		this.maxIterations = 20;
		
		//Default number of features
		this.numFactors = 8;
		
		//Default values of various model parameters which can be defined by user
		this.itemFactorStep = 0.001;
		this.itemFactorReg = 0.001;
		this.userFactorStep = 0.001;
		this.userFactorReg = 0.001;
		this.itemBiasReg = 0.001;
		this.itemBiasStep = 0.001;
		this.userBiasReg = 0.001;
		this.userBiasStep = 0.001;
		this.stepDec = 0.9;
	}
	
	public void parseParameters() {
		//TODO: Parse parameters from the json string and set the values
	}

	public void serializeMM(String dir) {
		String comment = "Latent factors for SVDPP";
		IO.mmOutputMatrix(dir+"SVDPP_latent_factors.mm" , 0, numUsers + numItems, latentFactors, comment);
		System.err.println("SerializeOver at "+ dir + "SVDPP_latent_factors.mm");
		
		String commentWeight = "Weight for SVDPP";
		IO.mmOutputMatrix(dir+"SVDPP_weights.mm" , 0, numUsers + numItems, weights, commentWeight);
		System.err.println("SerializeOver at "+ dir + "SVDPP_weights.mm");
		
		String commentBias = "Bias for SVDPP";
		IO.mmOutputVector(dir+"SVDPP_bias.mm" , 0, numUsers + numItems, bias, commentBias);
		System.err.println("SerializeOver at "+ dir + "SVDPP_bias.mm");		
	}

	
	public void initParameterValues(DataSetDescription dataMetadata) {
		if(!serialized){
        	numUsers = dataMetadata.getNumUsers();
        	numItems = dataMetadata.getNumItems();
        	latentFactors = new HugeDoubleMatrix(this.numUsers + this.numItems, numFactors);
        	latentFactors.randomize(0.0, 1.0);
        	weights = new HugeDoubleMatrix(this.numUsers + this.numItems, numFactors, 0.0);
        	bias = new ArrayRealVector(this.numUsers + this.numItems);
		}
	}

	@Override
	public double predict(int userId, int itemId, SparseVector userFeatures,
			SparseVector itemFeatures, SparseVector edgeFeatures,
			DataSetDescription datasetDesc) {
		// \hat(r_ui) = \mu +
		double prediction = this.globalMean;
		
		// + b_u + b_i
		prediction += this.bias.getEntry(userId) + this.bias.getEntry(itemId);
		
		// + q_i^T*(p_u + sum y_j/sqrt(|N(u)|))
		for(int f = 0; f < this.numFactors; f++) {
			prediction += this.latentFactors.getValue(itemId, f)*(
					this.latentFactors.getValue(userId, f) + this.weights.getValue(userId, f));
		}
		
		prediction = Math.max(prediction, datasetDesc.getMinval());
		prediction = Math.min(prediction, datasetDesc.getMaxval());
		
		return prediction;
	}

	@Override
	public int getEstimatedMemoryUsage(DataSetDescription datasetDesc) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public void serialize(String dir) {
	    //TODO: This is not a good way to create a path. Use some library to join into a path
        String filename = dir + this.id;

        try{
			SerializationUtils.serializeParam(filename, this);
		}catch(Exception i){
			System.err.println("Serialization Fails at" + filename);
		}
	}
	
	public static SVDPPParams deserialize(String file) throws IOException, ClassNotFoundException{
		SVDPPParams params = null;
		System.err.println("File:"+file);	  
	    FileInputStream fileIn = new FileInputStream(file);
	    ObjectInputStream in = new ObjectInputStream(fileIn);
	    params = (SVDPPParams) in.readObject();
	    in.close();
	    fileIn.close();
	    params.setSerializedTrue();
	    return params;
	}
	
}

public class SVDPP implements RecommenderAlgorithm {
	SVDPPParams params;
	DataSetDescription dataSetDescription;
	
	protected Logger logger = ChiLogger.getLogger("SVDPP");
	
	private double train_rmse;
	
	int iterationNum;
	
	public SVDPP(DataSetDescription datasetDesc, ModelParameters parameters) {
		//Initialize the model parameters
		this.params = (SVDPPParams)parameters;
		this.dataSetDescription = datasetDesc;
		
		//metadataMap contains global information computed by sharder?
		this.train_rmse = 0;
		
		this.iterationNum = 0;
	}
	
	@Override
	public void update(ChiVertex<Integer, RatingEdge> vertex, GraphChiContext context) {
		//On first iteration just compute the globalMean, number of users and number of items.
		//TODO: Is computing these values here the right thing to do?
		if(this.iterationNum == 0) {
			if(vertex.numOutEdges() > 0) {
				double sum = 0;
				for(int e = 0; e < vertex.numOutEdges(); e++) 
					sum +=  vertex.getOutEdgeValue(e).observation;

				synchronized (params) {
					params.numUsers++;
					params.globalMean += sum;
				}
			} else {
				synchronized (params) {
					params.numItems++;
				}
			}
			return;
		}
		
		//From second iteration onwards start the actual SGD steps.
		if(vertex.numOutEdges() > 0) {
			int user = context.getVertexIdTranslate().backward(vertex.getId());
			//sqrt(1/|N(u)|)
			double usrNorm = 1.0/Math.sqrt(vertex.numOutEdges());
			
			//Allocating various arrays which are used later. This avoids creation of many objects and GC overhead
			double[] itemWeight = new double[params.numFactors ];
			double[] itemFactors = new double[params.numFactors];
			double[] nbrItemWt = new double[params.numFactors];
			
			// Computing the value of sum_j(y_j) * (1/sqrt(N(u))) for first iteration.
			double[] userFactors = new double[params.numFactors];
			params.latentFactors.getRow(user, userFactors);

			double[] sumWeights = new double[params.numFactors];
			for(int i = 0; i < vertex.numOutEdges(); i++) {
				int item = context.getVertexIdTranslate().backward(vertex.getOutEdgeId(i));
				Arrays.fill(itemWeight, 0);
				params.weights.getRow(item, itemWeight);
				for(int f = 0; f < params.numFactors; f++)
					sumWeights[f] += itemWeight[f];
			}
			
			for(int f = 0; f < params.numFactors; f++) {
				sumWeights[f] *= usrNorm;
				params.weights.setValue(user, f, sumWeights[f]);
			}
			
	        // main algorithm, see Koren's paper, just below below equation (16)
	        for(int e=0; e < vertex.numOutEdges(); e++) {
	        	//User vertex.
	        	int item = context.getVertexIdTranslate().backward(vertex.getOutEdgeId(e));
	        	Arrays.fill(itemFactors, 0);
	        	params.latentFactors.getRow(item, itemFactors);
	        	
	        	float observation = vertex.getOutEdgeValue(e).observation;
	        	
	        	//TODO: Change this to use the predict method in ModelParameters.
	        	double estScore = this.params.predict(user, item, null, null, null, this.dataSetDescription);
	        	//double estScore = predict(user, item, observation, sumWeights);
	        	
	        	 // e_ui = r_ui - \hat{r_ui}
	        	double err = observation - estScore;
	        	
	        	//q_i = q_i + gamma2*(e_ui*(p_u + sum_j y_j/sqrt(N(U))) - lambda7*q_i)
	        	for (int f=0; f< params.numFactors; f++) { 
	        		itemFactors[f] = itemFactors[f] + params.itemFactorStep*(
	        				err*(userFactors[f] + sumWeights[f]) -
	        				params.itemFactorReg*itemFactors[f]
	        			); 
	        				
	        		params.latentFactors.setValue(item, f, itemFactors[f]);
	        	}
	        	
	        	//p_u = p_u + gamma2*(e_ui*q_i - lambda7*p_u)
	        	for (int f=0; f< params.numFactors; f++) {
	        		userFactors[f] = userFactors[f] + 
	        			params.userFactorStep*(
	        				err*itemFactors[f] - params.userFactorReg*userFactors[f]
	        			);
	        		params.latentFactors.setValue(user, f, userFactors[f]);
	        	}
	        	
	            //b_i = b_i + gamma1*(e_ui - gmma6 * b_i) 
	        	params.bias.setEntry(item, params.bias.getEntry(item)+ params.itemBiasStep*(err-params.itemBiasReg*params.bias.getEntry(item)));
	        	//b_u = b_u + gamma1*(e_ui - gamma6 * b_u)
	        	params.bias.setEntry(user, params.bias.getEntry(user)+ params.itemBiasStep*(err-params.itemBiasReg*params.bias.getEntry(user)));
	        	
	        	synchronized (this) {
					this.train_rmse += err*err;
				}
	     
	        	//Calculate sum_j(y_j) * (1/sqrt(N(u))) for the next r_ui.
	        	//TODO: Clarify from Danny: In C++ version, the same value of
	        	//sum_j(y_j) * (1/sqrt(N(u))) is used for each of the ratings of a user in
	        	//one iteration and the item.weights are updated after all ratings have been
	        	//seen for a user in that iteration. This seems to differ from the paper, 
	        	//wherein, for each rating r_ui, the weights y_j are updated. However, the results
	        	//are similar in training RMSE.
	        	for(int f = 0; f < params.numFactors; f++) {
	        		//Persist the sum of weights and get ready for next iteration.
	        		this.params.weights.setValue(user, f, sumWeights[f]);
	    			sumWeights[f] = 0;
	    		}
	        	//For all neighbors of u
	        	//y_j = y_j  +   gamma2*(e_ui * (1/sqrt|N(u)|) * q_i - gamma7 * y_j)
	        	for(int i = 0; i < vertex.numOutEdges(); i++) {
	        		Arrays.fill(nbrItemWt, 0);
	        		int itemWtIndex = context.getVertexIdTranslate().backward(vertex.getOutEdgeId(i));

		        	params.weights.getRow(itemWtIndex, nbrItemWt);
		        	for(int f = 0; f < params.numFactors; f++) {
		        		double tmp = params.itemFactorStep*(err*usrNorm*itemFactors[f] - params.itemFactorReg*nbrItemWt[f]);
		        		nbrItemWt[f] = nbrItemWt[f] + tmp;
		        		params.weights.setValue(itemWtIndex, f, nbrItemWt[f]);
		        		
		        		//For the next iteration
		        		sumWeights[f] += nbrItemWt[f];
		        	}
	        	}
	        	
				for(int f = 0; f < params.numFactors; f++)
					sumWeights[f] *= usrNorm;
	        }
	        
		}
	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		 //On first iteration, initialize the vertices in memory.
    	this.train_rmse = 0;

    	//Since iteration number 0 was used to compute global mean, numUsers and numItems.
        if (this.iterationNum == 1) {
        	   params.initParameterValues(dataSetDescription);
        	   params.globalMean = params.globalMean / ctx.getNumEdges();
        }		
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		if (this.iterationNum >= 1) {
			this.train_rmse = Math.sqrt(this.train_rmse / (1.0 * ctx.getNumEdges()));
	        this.logger.info("Train RMSE: " + this.train_rmse);
	        
	        //Reduce all steps.
	        params.itemBiasStep *= params.stepDec;
	        params.userBiasStep *= params.stepDec;
	        params.itemFactorStep *= params.stepDec;
	        params.userFactorStep *= params.stepDec;
		}
		this.iterationNum++;
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
	
	@Override
	public ModelParameters getParams() {
		// TODO Auto-generated method stub
		return this.params;
	}

	@Override
	public boolean hasConverged(GraphChiContext ctx) {
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
