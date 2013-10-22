package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.commons.math3.distribution.NormalDistribution;

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
import gov.sandia.cognition.math.matrix.VectorEntry;
import gov.sandia.cognition.math.matrix.mtj.SparseMatrixFactoryMTJ;
import gov.sandia.cognition.math.matrix.mtj.SparseRowMatrix;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;
import gov.sandia.cognition.math.matrix.mtj.SparseVectorFactoryMTJ;

/**
 * This is the implementation of Factorization Machines using the MCMC method
 * based on the algorithm given in the following paper:
 * "Steffen Rendle (2012): Factorization Machines with libFM, in ACM Trans. Intell. Syst. Technol., 3(3), May"
 * 
 * @author mayank
 *
 */

class LibFM_SGDParams extends ModelParameters {

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

	int D;			//Number of latent features 
	
	int num_users;
	int num_items;
	int num_user_features;
	int num_item_features;
	int num_edge_features;
	int num_features;
	
	public LibFM_SGDParams(String id, String json) {
		super(id, json);
		
		setDefaults();
		
		parseJsonParams(json);
	}
	
	private void parseJsonParams(String json) {

	}

	private void setDefaults() {
		this.D = 10;
		this.lambda_0 = 0.01;
		this.lambda_w = 0.01;
		this.lambda_v = new double[this.D];
		for(int i = 0; i < this.D; i++) {
			this.lambda_v[i] = 0.05;
		}

		this.eta = 0.005;
		this.init_dev = 0.1;
		
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
		//Parameters. (Should this be stored in memory or should it be stored in the vertex / edge?
		this.w_0 = 0;									//0-way interactions
		this.w = new double[this.num_features];			//1-way interactions
		this.v = new double[this.num_features][this.D];	//2-way interactions
		
		NormalDistribution  dist = new NormalDistribution(0, 0.1);
		for(int j = 0; j < this.num_features; j++) {
			this.w[j] = 0;
			for(int f = 0; f < this.D; f++) {
				this.v[j][f] = dist.sample();
			}
		}
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


public class LibFM_SGD  implements GraphChiProgram<Integer, LibFMEdge> {
	private ProblemSetup setup;
	private LibFM_SGDParams  params;
	
	//Contains data about user and item features. Currently this is held in memory.
	SparseRowMatrix features;

	protected Logger logger = ChiLogger.getLogger("LibFM_MCMC");
	
	//Train RMSE
	double train_rmse;
	
	//Contains aggregate of all the samples generated in the iteration for testing.
	TestDataSamples testSamples;
	
	public LibFM_SGD(ProblemSetup setup, ModelParameters par) {
		this.params = (LibFM_SGDParams)par;
		this.setup = setup;
	}
	
	public double predict(SparseVector row) {
		//y = w0 +
		double estVal = this.params.w_0;
		
		Iterator<VectorEntry> it = row.iterator();
		while(it.hasNext()) {
			VectorEntry vec = it.next();
			int i = vec.getIndex();
			double xi = vec.getValue();
			double wi = this.params.w[i];
			double[] vi = this.params.v[i];  
			
			// wi*xi +
			estVal += wi*xi;
			if(Double.isNaN(estVal)) {
				//System.out.println();
			}
			
			Iterator<VectorEntry> it2 = row.iterator();
			while(it2.hasNext()) {
				vec = it2.next();
				int j = vec.getIndex();
				double xj = vec.getValue();
				double[] vj = this.params.v[j];
				double dotProd = 0;
				for(int f = 0; f < this.params.D; f++) {
					dotProd += vi[f]*vj[f];
				}
				
				// <vi, vj>*xi*xj +
				estVal += dotProd*xi*xj;
				if(Double.isNaN(estVal)) {
					//System.out.println();
				}
			}
		}
		estVal = estVal/2;

		estVal = Math.max(estVal, this.setup.minval);
		estVal = Math.min(estVal, this.setup.maxval);

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
			int user = context.getVertexIdTranslate().backward(vertex.getId());
			SparseVector userFeatures = this.features.getRow(user);
			
			for(int e = 0; e < vertex.numOutEdges(); e++) {
				int item = context.getVertexIdTranslate().backward(vertex.getOutEdgeId(e));
				
				SparseVector itemFeatures = this.features.getRow(item);
				LibFMEdge edge = vertex.edge(e).getValue();

				SparseVector allFeatures = createAllFeatureVec(user, item, 
						userFeatures, itemFeatures);
				
				//TODO: Set features representing edge attributes (like time stamp)
				double estVal = predict(allFeatures);
				double err = edge.observation - estVal; 

				//Compute sum vj,f * xj for this observations
				Iterator<VectorEntry> it = allFeatures.iterator();
				double[] sum_v_x = new double[this.params.D];
				while(it.hasNext()) {
					VectorEntry vec = it.next();
					int j = vec.getIndex();
					double xj = vec.getValue();
					for(int f = 0; f < this.params.D; f++) {
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
					
					for(int f = 0; f < this.params.D; f++) {
						double old_vjf_x = this.params.v[j][f]*xj;
						this.params.v[j][f] = this.params.v[j][f] - this.params.eta*(
								-2*err *xj*(sum_v_x[f] - this.params.v[j][f]*xj) +
								2*this.params.lambda_v[f]*this.params.v[j][f]
							); 
						//TODO: Should this be updated?
						sum_v_x[f] += this.params.v[j][f]*xj - old_vjf_x;  
					}
				}
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
			readFeatures(getUserBase(), getUserFeatureBase(), this.setup.userFeatures, true);
			
			//Read Item features
			readFeatures(getItemBase(), getItemFeatureBase(), this.setup.itemFeatures, false);
		}
		
		this.train_rmse = 0;
		
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
	
	/**
	 * 
	 * @param vertexBase
	 * @param featureBase
	 * @param fileName
	 * @return
	 */
	private void readFeatures(int vertexBase, int featureBase, String fileName, boolean isUser) {
		if(fileName == null)
			return;
		BufferedReader br = null;
		
		try {
			br = new BufferedReader(new FileReader(new File(fileName)));
			String line;
			
			while( (line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				int vertexId = vertexBase + Integer.parseInt(tokens[0]);
				for(int i = 1; i < tokens.length; i++) {
					int featureId = featureBase + Integer.parseInt(tokens[i].split(":")[0]);
					double featureVal = Double.parseDouble(tokens[i].split(":")[1]);
					features.setElement(vertexId, featureId, featureVal);
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
        
        LibFM_SGDParams params = new LibFM_SGDParams(problemSetup.getRunId("LibFM_SGD"), problemSetup.paramJson);
        GraphChiProgram<Integer, LibFMEdge> libfm = new LibFM_SGD(problemSetup, params);
        
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
    	  ByteBuffer buf = ByteBuffer.allocate(sizeOf());
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

