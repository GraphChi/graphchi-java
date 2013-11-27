package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math3.distribution.NormalDistribution;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.RatingEdge;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.VertexDataCache;
import gov.sandia.cognition.math.matrix.VectorEntry;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;
import gov.sandia.cognition.math.matrix.mtj.SparseVectorFactoryMTJ;

/**
 * This is the implementation of Factorization Machines using the SGD method
 * based on the algorithm given in the following paper:
 * "Steffen Rendle (2012): Factorization Machines with libFM, in ACM Trans. Intell. Syst. Technol., 3(3), May"
 * 
 * @author mayank
 *
 */

class LibFM_SGDParams extends ModelParameters  {
	public static final String LAMBDA_0_KEY = "lambda_0";
	public static final String LAMBDA_W_KEY = "lambda_w";
	public static final String LAMBDA_V_KEY = "lambda_v";
	public static final String NUM_LATENT_FACTORS_KEY = "latentFactors";
	public static final String ETA_KEY = "eta";
	public static final String INIT_DEV_KEY = "init_dev";
	
	// Number of iterations - Stopping condition. 
	//TODO: Note that may be we can have a better stopping condition based on change in training RMSE.  
	int maxIterations;
	
	//The standard deviation of the normal distribution to be used for initializing
	//the parameters of V.
	double init_dev;
	
	//Regularization Parameters
	double lambda_0;
	double lambda_w;
	double[] lambda_v;
	//Learn Rate
	double eta;
	
	//Parameters. (Should this be stored in memory or should it be stored in the vertex / edge?
	double w_0;		//0-way interactions
	double[] w;		//1-way interactions
	double[][] v;	//2-way interactions

	int numFactors;			//Number of latent features 
	
	public LibFM_SGDParams(String id, Map<String, String> paramsMap) {
		super(id, paramsMap);
		
		setDefaults();
		
		parseJsonParams();
	}
	
	private void parseJsonParams() {
		
		if(this.paramsMap.get(NUM_LATENT_FACTORS_KEY) != null) {
			this.numFactors = Integer.parseInt(this.paramsMap.get(NUM_LATENT_FACTORS_KEY));
		}
		if(this.paramsMap.get(LAMBDA_0_KEY) != null) {
			this.lambda_0 = Float.parseFloat(this.paramsMap.get(LAMBDA_0_KEY));
		}
		if(this.paramsMap.get(LAMBDA_W_KEY) != null) {
			this.lambda_w = Float.parseFloat(this.paramsMap.get(LAMBDA_W_KEY));
		}
		if(this.paramsMap.get(LAMBDA_V_KEY) != null) {
			float lam_v = Float.parseFloat(this.paramsMap.get(LAMBDA_V_KEY));
			this.lambda_v = new double[this.numFactors];
			for(int i = 0; i < this.numFactors; i++) {
				this.lambda_v[i] = lam_v;
			}
		}
		if(this.paramsMap.get(ETA_KEY) != null) {
			this.eta = Float.parseFloat(this.paramsMap.get(ETA_KEY));
		}
		if(this.paramsMap.get(INIT_DEV_KEY) != null) {
			this.init_dev = Float.parseFloat(this.paramsMap.get(INIT_DEV_KEY));
		}
	}

	private void setDefaults() {
		this.maxIterations = 20;
		
		this.numFactors = 8;
		this.lambda_0 = 0.15;
		this.lambda_w = 0.15;
		this.lambda_v = new double[this.numFactors];
		for(int i = 0; i < this.numFactors; i++) {
			this.lambda_v[i] = 0.15;
		}

		this.eta = 0.001;
		this.init_dev = 0.1;
		
	}
	
	public void initParameters(DataSetDescription datasetDesc) {
        int numFeatures = datasetDesc.getNumItemFeatures() + 
                datasetDesc.getNumUserFeatures() + datasetDesc.getNumRatingFeatures() + 
                datasetDesc.getNumUsers() + datasetDesc.getNumItems() + 1;
	    
		//Parameters. (Should this be stored in memory or should it be stored in the vertex / edge?
		this.w_0 = 0;									//0-way interactions
		this.w = new double[numFeatures];			//1-way interactions
		this.v = new double[numFeatures][this.numFactors];	//2-way interactions

		//Initialize 2 way interations
		NormalDistribution  dist = new NormalDistribution(0, 0.1);
		for(int j = 0; j < numFeatures; j++) {
			this.w[j] = 0;
			for(int f = 0; f < this.numFactors; f++) {
				this.v[j][f] = dist.sample();
			}
		}
	}

	@Override
	public void serialize(String dir) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public double predict(int userId, int itemId, SparseVector userFeatures,
			SparseVector itemFeatures, SparseVector edgeFeatures,
			DataSetDescription datasetDesc) {
		SparseVector row = createAllFeatureVec(userId, itemId, 
				userFeatures, itemFeatures, datasetDesc);
		return predict(row, datasetDesc);
	}
	
	public double predict(SparseVector row, DataSetDescription datasetDesc) {
		//y = w0 +
		double estVal = this.w_0;
		
		double sumTwoWay = 0;
		Iterator<VectorEntry> it = row.iterator();
		while(it.hasNext()) {
			VectorEntry vec = it.next();
			int i = vec.getIndex();
			double xi = vec.getValue();
			double wi = this.w[i];
			double[] vi = this.v[i];  
			
			// wi*xi +
			estVal += wi*xi;
			
			Iterator<VectorEntry> it2 = row.iterator();
			while(it2.hasNext()) {
				vec = it2.next();
				int j = vec.getIndex();
				if(j <= i)
					continue;
				double xj = vec.getValue();
				double[] vj = this.v[j];
				double dotProd = 0;
				for(int f = 0; f < this.numFactors; f++) {
					dotProd += vi[f]*vj[f];
				}
				
				// <vi, vj>*xi*xj +
				sumTwoWay += dotProd*xi*xj;
			}
		}
		estVal = estVal + sumTwoWay;

		return estVal;
	}
	
	public SparseVector createAllFeatureVec(int user, int item, SparseVector userFeatures, 
			SparseVector itemFeatures, DataSetDescription datasetDesc) {
		//Construct a row of the design matrix.
		int numTotalFeatures = getEdgeFeaturesBase(datasetDesc) + datasetDesc.getNumRatingFeatures() + 1; 
		SparseVector allFeatures = (new SparseVectorFactoryMTJ()).createVector(numTotalFeatures);
		
		//Set feature representing an user.
		allFeatures.setElement(user, 1);
		//Set feature representing an item.
		allFeatures.setElement(item, 1);
		
		Iterator<VectorEntry> it;
		//Set features representing user attributes.
		if(userFeatures != null) {
			it = userFeatures.iterator();
			while(it.hasNext()) {
				VectorEntry feature = it.next();
				int featureIndex = getUserFeatureBase(datasetDesc) + feature.getIndex(); 
				allFeatures.setElement(featureIndex, feature.getValue());
			}
		}
		
		//Set features representing item attributes.
		if(itemFeatures != null) {
			it = itemFeatures.iterator();
			while(it.hasNext()) {
				VectorEntry feature = it.next();
				int featureIndex = getItemFeatureBase(datasetDesc) + feature.getIndex();
				allFeatures.setElement(featureIndex, feature.getValue());
			}
		}
		
		return allFeatures;
	}
	
	private int getUserBase(DataSetDescription datasetDesc){
		return 0;
	}
	
	private int getItemBase(DataSetDescription datasetDesc) {
		return getUserBase(datasetDesc) + datasetDesc.getNumUsers();
	}
	
	private int getUserFeatureBase(DataSetDescription datasetDesc) {
		return getItemBase(datasetDesc) + datasetDesc.getNumItems();
	}
	
	private int getItemFeatureBase(DataSetDescription datasetDesc) {
		return getUserFeatureBase(datasetDesc) + datasetDesc.getNumUserFeatures();
	}
	
	private int getEdgeFeaturesBase(DataSetDescription datasetDesc) {
		return getItemFeatureBase(datasetDesc) + datasetDesc.getNumItemFeatures();
	}
	
	@Override
	public int getEstimatedMemoryUsage(DataSetDescription datasetDesc) {
        int numFeatures = datasetDesc.getNumItemFeatures() + 
                datasetDesc.getNumUserFeatures() + datasetDesc.getNumRatingFeatures() + 
                datasetDesc.getNumUsers() + datasetDesc.getNumItems();

        //Compute estimated memory usage.
        //1 way factors.
        int estimatedMemoryUsage = 8*numFeatures / (1024*1024) ;
        //2 way factors.
        estimatedMemoryUsage += (8*numFeatures*this.numFactors) / (1024*1024);
        //1 MB Slack
        estimatedMemoryUsage += 1;
	    
		return estimatedMemoryUsage;
	}
	
}

public class LibFM_SGD  implements RecommenderAlgorithm  {
	DataSetDescription datasetDesc;
	LibFM_SGDParams  params;
	
	//Contains data about user and item features. Currently this is held in memory.
	VertexDataCache vertexDataCache = null;
	
	int iterationNum;

	protected Logger logger = ChiLogger.getLogger("LibFM_SGD");
	
	//Train RMSE
	double train_rmse;
	
	public LibFM_SGD(DataSetDescription dataDesc, ModelParameters par) {
		this.params = (LibFM_SGDParams)par;
		this.datasetDesc = dataDesc;
		
		this.iterationNum = 0;
	}
	
	@Override
	public void update(ChiVertex<Integer, RatingEdge> vertex, GraphChiContext context) {
		if(vertex.numOutEdges() > 0) {
			//User vertex
			
			//Update user feature aggregates.
			int user = context.getVertexIdTranslate().backward(vertex.getId());
			//SparseVector userFeatures = this.features.getRow(user);
			
			SparseVector userFeatures = null;
			if(this.vertexDataCache != null) {
				userFeatures = this.vertexDataCache.getFeatures(user);
			}
			
			for(int e = 0; e < vertex.numOutEdges(); e++) {
				int item = context.getVertexIdTranslate().backward(vertex.getOutEdgeId(e));
				
				SparseVector itemFeatures = null;
				if(this.vertexDataCache != null) {
					itemFeatures = this.vertexDataCache.getFeatures(item);
				}
				
				RatingEdge edge = vertex.edge(e).getValue();

				SparseVector allFeatures = this.params.createAllFeatureVec(user, item, 
						userFeatures, itemFeatures, this.datasetDesc);
				
				//TODO: Set features representing edge attributes (like time stamp)
				double estVal = this.params.predict(allFeatures, this.datasetDesc);
				double err = edge.observation - estVal; 

				//Compute sum vj,f * xj for this observations
				Iterator<VectorEntry> it = allFeatures.iterator();
				double[] sum_v_x = new double[this.params.numFactors];
				while(it.hasNext()) {
					VectorEntry vec = it.next();
					int j = vec.getIndex();
					double xj = vec.getValue();
					for(int f = 0; f < this.params.numFactors; f++) {
						sum_v_x[f] += this.params.v[j][f]*xj;
					}
				}
				
				//Take a gradient step for the parameters.
				this.params.w_0 = this.params.w_0 - this.params.eta*(-2*err + 2*this.params.lambda_0*this.params.w_0);
				
				it = allFeatures.iterator();
				while(it.hasNext()) {
					VectorEntry vec = it.next();
					int j = vec.getIndex();
					double xj = vec.getValue();
					this.params.w[j] = this.params.w[j] - this.params.eta*(
							-2*err*xj + 2*this.params.lambda_w*this.params.w[j]);
					
					for(int f = 0; f < this.params.numFactors; f++) {
						double old_vjf_x = this.params.v[j][f]*xj;
						this.params.v[j][f] = this.params.v[j][f] - this.params.eta*(
								-2*err *xj*(sum_v_x[f] - this.params.v[j][f]*xj) +
								2*this.params.lambda_v[f]*this.params.v[j][f]
							); 
						//TODO: Should this be updated?
						sum_v_x[f] += this.params.v[j][f]*xj - old_vjf_x;  
					}
				}
				estVal = Math.max(estVal, datasetDesc.getMinval());
				estVal = Math.min(estVal, datasetDesc.getMaxval());
				this.train_rmse += err*err; 
			}
				
				
		} else {
			//Item vertex
		}
	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		if(this.iterationNum == 0) {
			this.params.initParameters(this.datasetDesc);
		}
		
		this.train_rmse = 0;
		
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		
		this.train_rmse = Math.sqrt(this.train_rmse / (1.0 * ctx.getNumEdges()));
        this.logger.info("Train RMSE: " + this.train_rmse);
        
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
		return this.datasetDesc;
	}
	
	@Override
	public int getEstimatedMemoryUsage() {
		return this.params.getEstimatedMemoryUsage(this.datasetDesc);
	}
	
}

