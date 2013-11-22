package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.BlockRealMatrix;
import org.apache.commons.math.linear.CholeskyDecompositionImpl;
import org.apache.commons.math.linear.NotPositiveDefiniteMatrixException;
import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.linear.RealVector;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.SerializationUtils;
import edu.cmu.graphchi.util.HugeDoubleMatrix;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;

/**
 * Matrix factorization with the Alternative Least Squares (ALS) algorithm.
 * This code is based on GraphLab's implementation of ALS by Joey Gonzalez
 * and Danny Bickson (CMU). A good explanation of the algorithm is
 * given in the following paper:
 *    Large-Scale Parallel Collaborative Filtering for the Netflix Prize
 *    Yunhong Zhou, Dennis Wilkinson, Robert Schreiber and Rong Pan
 *    http://www.springerlink.com/content/j1076u0h14586183/
 *
 *
 * This version stores the latent factors in memory and thus requires
 * sufficient memory to store D floating point numbers for each vertex.
 * D is a dimensionality factor (default 5).
 *
 * Each edge stores a "rating" and the purpose of this algorithm is to
 * find a matrix factorization U x V so that U x V approximates the rating
 * matrix R.
 *
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, 2013
 * @author Modifications by Danny Bickson, CMU, 2013
 * @author Modifications by Mayank Mohta (mmohta@andrew.cmu.edu), CMU, 2013
 */

class ALSParams extends ModelParameters {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6384331093067321704L;
	private static final String LAMBDA_KEY = "regularization";
	private static final String NUM_LATENT_FACTORS_KEY = "num_latent_factors";
	
	double lambda;	//regularization
	// Number of iterations - Stopping condition. 
	//TODO: Note that may be we can have a better stopping condition based on change in training RMSE.  
	int maxIterations;	
	int numFactors;	//number of features
	HugeDoubleMatrix latentFactors;
	
	int numUsers, numItems;
	
	public ALSParams(String id, Map<String, String> paramsMap) {
		super(id, paramsMap);
		
		setDefaults();
		
		parseParameters();
		
	}
	
	public void setDefaults() {
		this.lambda = 0.065;
		this.maxIterations = 20;
		this.numFactors = 10;
	}
	
	public void parseParameters() {
		if(this.paramsMap.containsKey(LAMBDA_KEY)) {
			this.lambda = Double.parseDouble(this.paramsMap.get(LAMBDA_KEY));
		}
		if(this.paramsMap.containsKey(NUM_LATENT_FACTORS_KEY)) {
			this.numFactors = Integer.parseInt(this.paramsMap.get(NUM_LATENT_FACTORS_KEY));
		}
	}
	
    void initParameterValues(long size, int D, DataSetDescription dataMetadata){

    	if(!serialized){
        	numUsers = dataMetadata.getNumUsers();
        	numItems = dataMetadata.getNumItems();
    		latentFactors = new HugeDoubleMatrix(size, D);
            /* Fill with random data */
            latentFactors.randomize(0.0, 1.0);
    	}    
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

	public static ALSParams deserialize(String file) throws IOException, ClassNotFoundException{
		ALSParams params = null;
		System.err.println("File:"+file);	  
	    FileInputStream fileIn = new FileInputStream(file);
	    ObjectInputStream in = new ObjectInputStream(fileIn);
	    params = (ALSParams) in.readObject();
	    in.close();
	    fileIn.close();
	    params.setSerializedTrue();
	    return params;
	}

	public void serializeMM(String dir){
		String comment = "Latent factors for ALS";
		IO.mmOutputMatrix(dir+"ALS_latent_factors_lambda_"+lambda+"_factor_"+numFactors+".mm" , 0, numUsers + numItems, latentFactors, comment);
		System.err.println("SerializeOver at "+ dir + "ALS_latent_factors.mm");
	}

	@Override
	public double predict(int originalUserId, int originalItemId, SparseVector userFeatures,
			SparseVector itemFeatures, SparseVector edgeFetures, DataSetDescription datasetDesc) {
		RealVector userFactors = this.latentFactors.getRowAsVector(originalUserId);
		RealVector itemFactors = this.latentFactors.getRowAsVector(originalItemId);
		
		double prediction = userFactors.dotProduct(itemFactors);
    	prediction = Math.min(prediction, datasetDesc.getMaxval());
    	prediction = Math.max(prediction, datasetDesc.getMinval());
    	return prediction;
	}
	
	@Override
	public int getEstimatedMemoryUsage(DataSetDescription datasetDesc) {
		int estimatedMemory = HugeDoubleMatrix.getEstimatedMemory(datasetDesc.getNumUsers(),
				datasetDesc.getNumItems());
		//Add 1 Mb of slack (Huge double matrix assigns memory in 1 Mb chunks.)
		estimatedMemory += 1;
		return estimatedMemory;
	}

}

public class ALS implements RecommenderAlgorithm {
    
	DataSetDescription dataMetadata;
	ALSParams params;
	protected Logger logger = ChiLogger.getLogger("ALS");
    double train_rmse = 0.0;
    
    int iterationNum;
    
    public ALS(DataSetDescription dataMetadata, ModelParameters params) {
    	this.dataMetadata = dataMetadata;
    	this.params = (ALSParams)params;
    	this.iterationNum = 0;
    }

    
    @Override
    public void update(ChiVertex<Integer, RatingEdge> vertex, GraphChiContext context) {
        if (vertex.numEdges() == 0) return;

        int vertexId = context.getVertexIdTranslate().backward(vertex.getId());
        
        RealMatrix XtX = new BlockRealMatrix(params.numFactors, params.numFactors);
        RealVector Xty = new ArrayRealVector(params.numFactors);
        RealVector latent_factor = params.latentFactors.getRowAsVector(vertexId);

        try {
            double squaredError = 0;
            boolean is_user = vertex.numOutEdges() > 0;
            // Compute XtX and Xty (NOTE: unweighted)
            for(int e=0; e < vertex.numEdges(); e++) {
                float observation = vertex.edge(e).getValue().observation;
                int nbrId = context.getVertexIdTranslate().backward(vertex.edge(e).getVertexId());
                RealVector neighbor = params.latentFactors.getRowAsVector(nbrId);
            
                //Xty.add(neighbor * observation);
                //XtX.add(neighbor.outerProduct(neighbor));
                for(int i=0; i < params.numFactors; i++) {
                    Xty.setEntry(i, Xty.getEntry(i) + neighbor.getEntry(i) * observation);
                    for(int j=i; j < params.numFactors; j++) {
                        XtX.setEntry(j,i, XtX.getEntry(j, i) + neighbor.getEntry(i) * neighbor.getEntry(j));
                    }
                }
                
                // Symmetrize
                for(int i=0; i < params.numFactors; i++) {
                    for(int j=i+1; j< params.numFactors; j++) XtX.setEntry(i,j, XtX.getEntry(j, i));
                }
                
                if (is_user){
                  double prediction = this.params.predict(vertexId, nbrId, null, null, null, this.dataMetadata);
                  squaredError += Math.pow(prediction - observation,2);
                }
            }
            
            // Diagonal -- add regularization
            for (int i=0; i < params.numFactors; i++) 
            	XtX.setEntry(i, i, XtX.getEntry(i, i) + this.params.lambda * vertex.numEdges());

            // Solve the least-squares optimization using Cholesky Decomposition
            RealVector newLatentFactor = new CholeskyDecompositionImpl(XtX).getSolver().solve(Xty);
            params.latentFactors.setRow(vertexId, newLatentFactor.getData());

           if (is_user){
        	   synchronized (this) {
        		   this.train_rmse += squaredError;
               }
           }
            
        } catch (NotPositiveDefiniteMatrixException npdme) {
        	this.logger.warning("Matrix was not positive definite: " + XtX);
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }

    @Override
    public void beginIteration(GraphChiContext ctx) {
        /* On first iteration, initialize the vertices in memory.
         * Vertices' latent factors are stored in the vertexValueMatrix
         * so that each row contains one latent factor.
         */
    	this.train_rmse = 0;
        if (this.iterationNum == 0) {
        	params.initParameterValues(ctx.getNumVertices(), params.numFactors, dataMetadata);
        }
    }

    @Override
    public void endIteration(GraphChiContext ctx) {
    	 /* Output RMSE */
        this.train_rmse = Math.sqrt(this.train_rmse / (1.0 * ctx.getNumEdges()));
        this.logger.info("Train RMSE: " + this.train_rmse);
        this.iterationNum++;
    }

    @Override
    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    @Override
    public void endInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    @Override
    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    @Override
    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }
    
	@Override
	public ModelParameters getParams() {
		return this.params;
	}


	@Override
	public boolean hasConverged(GraphChiContext ctx) {
		return this.iterationNum == this.params.maxIterations;
	}


	@Override
	public DataSetDescription getDataSetDescription() {
		return this.dataMetadata;
	}
	
	@Override
	public int getEstimatedMemoryUsage() {
		return this.params.getEstimatedMemoryUsage(this.dataMetadata);
	}

}
