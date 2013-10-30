package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.util.List;
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
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
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
	public static final String LAMBDA_KEY = "regularization";
	public static final String NUM_LATENT_FACTORS_KEY = "num_latent_factors";
	
	double lambda;	//regularization
	int D;	//number of features
	HugeDoubleMatrix latentFactors;
	int numUsers;
	int numItems;
	
	public ALSParams(String id, Map<String, String> paramsMap) {
		super(id, paramsMap);
		
		setDefaults();
		
		parseParameters();
		
	}
	
	public void setDefaults() {
		this.lambda = 0.065;
		this.D = 10;
	}
	
	public void parseParameters() {
		if(this.paramsMap.get(LAMBDA_KEY) != null) {
			this.lambda = Float.parseFloat(this.paramsMap.get(LAMBDA_KEY));
		}
		if(this.paramsMap.get(NUM_LATENT_FACTORS_KEY) != null) {
			this.D = Integer.parseInt(this.paramsMap.get(NUM_LATENT_FACTORS_KEY));
		}
	}
	
    void initParameterValues(long size, int D){
        latentFactors = new HugeDoubleMatrix(size, D);

        /* Fill with random data */
        latentFactors.randomize(0f, 1.0f);
      }
	
	@Override
	public void serialize(String dir) {
		// TODO Auto-generated method stub
	}

	@Override
	public void deserialize(String file) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public double predict(int userId, int itemId, SparseVector userFeatures,
			SparseVector itemFeatures, SparseVector edgeFetures, DataSetDescription datasetDesc) {
		RealVector userFactors = this.latentFactors.getRowAsVector(userId);
		RealVector itemFactors = this.latentFactors.getRowAsVector(itemId);
		
		double prediction = userFactors.dotProduct(itemFactors);
    	prediction = Math.min(prediction, datasetDesc.getMaxval());
    	prediction = Math.max(prediction, datasetDesc.getMinval());
    	return prediction;
	}
}

public class ALS implements RecommenderAlgorithm {
    
	DataSetDescription dataMetadata;
	ALSParams params;
	protected Logger logger = ChiLogger.getLogger("ALS");
    double train_rmse = 0.0;
    
    public ALS(DataSetDescription dataMetadata, ModelParameters params) {
    	this.dataMetadata = dataMetadata;
    	this.params = (ALSParams)params;
    }

    
    @Override
    public void update(ChiVertex<Integer, RatingEdge> vertex, GraphChiContext context) {
        if (vertex.numEdges() == 0) return;

        int vertexId = context.getVertexIdTranslate().backward(vertex.getId());
        
        RealMatrix XtX = new BlockRealMatrix(params.D, params.D);
        RealVector Xty = new ArrayRealVector(params.D);
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
                for(int i=0; i < params.D; i++) {
                    Xty.setEntry(i, Xty.getEntry(i) + neighbor.getEntry(i) * observation);
                    for(int j=i; j < params.D; j++) {
                        XtX.setEntry(j,i, XtX.getEntry(j, i) + neighbor.getEntry(i) * neighbor.getEntry(j));
                    }
                }
                
                // Symmetrize
                for(int i=0; i < params.D; i++) {
                    for(int j=i+1; j< params.D; j++) XtX.setEntry(i,j, XtX.getEntry(j, i));
                }
                
                if (is_user){
                  double prediction = this.params.predict(vertexId, nbrId, null, null, null, this.dataMetadata);
                  squaredError += Math.pow(prediction - observation,2);
                }
            }
            
            // Diagonal -- add regularization
            for (int i=0; i < params.D; i++) 
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
        if (ctx.getIteration() == 0) {
        	params.initParameterValues(ctx.getNumVertices(), params.D);
        }
    }

    @Override
    public void endIteration(GraphChiContext ctx) {
    	 /* Output RMSE */
        this.train_rmse = Math.sqrt(this.train_rmse / (1.0 * ctx.getNumEdges()));
        this.logger.info("Train RMSE: " + this.train_rmse);
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
		// TODO Auto-generated method stub
		return this.params;
	}


	@Override
	public boolean hasConverged(GraphChiContext ctx) {
		// TODO Auto-generated method stub
		return ctx.getIteration() == ctx.getNumIterations() - 1;
	}


	@Override
	public DataSetDescription getDataSetDescription() {
		// TODO Auto-generated method stub
		return this.dataMetadata;
	}


    /**
     * Usage: java edu.cmu.graphchi.ALSMatrixFactorization <input-file> <nshards> <D>
     * Normally nshards of 10 or so is fine.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

    	ProblemSetup problemSetup = new ProblemSetup(args);
    	
    	DataSetDescription dataDesc = new DataSetDescription();
    	dataDesc.loadFromJsonFile(problemSetup.dataMetadataFile);

    	FastSharder<Integer, RatingEdge> sharder = AggregateRecommender.createSharder(dataDesc.getRatingsUrl(), 
				problemSetup.nShards, 0); 
		IO.convertMatrixMarket(dataDesc.getRatingsUrl(), problemSetup.nShards, sharder);
        
		List<RecommenderAlgorithm> algosToRun = RecommenderFactory.buildRecommenders(dataDesc, 
				problemSetup.paramFile, null);

		//Just run the first one. It should be ALS.
		if(!(algosToRun.get(0) instanceof ALS)) {
			System.out.println("Please check the parameters file. The first algo listed is not of type ALS");
			System.exit(2);
		}
		
		GraphChiProgram<Integer, RatingEdge> als = algosToRun.get(0);
		
        /* Run GraphChi */
        GraphChiEngine<Integer, RatingEdge> engine = new GraphChiEngine<Integer, RatingEdge>(dataDesc.getRatingsUrl(), problemSetup.nShards);
        
        engine.setEdataConverter(new RatingEdgeConvertor(0));
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        engine.setModifiesOutedges(false); // Important optimization
        engine.run(als, 5);

        ((ALS)als).params.serialize(problemSetup.outputLoc);
    }

    /**
     * Output in matrix market format
     * @param training
     * @param vertexIdTranslate
     * @throws Exception
     */
/*    private void writeOutputMatrices(VertexIdTranslate vertexIdTranslate) throws Exception {
    	Map<String, String> metaDataMap = FastSharder.readMetadata(this.problemSetup.training, this.problemSetup.nShards);
    	int numLeft = Integer.parseInt(metaDataMap.get("numLeft"));
    	int numRight = Integer.parseInt(metaDataMap.get("numRight"));
    	
         Output left 
        String leftFileName = this.problemSetup.training + "_U.mm";
        BufferedWriter wr = new BufferedWriter(new FileWriter(leftFileName));
        wr.write("%%MatrixMarket matrix array real general\n");
        wr.write(params.D + " " + numLeft + "\n");

        for(int j=0; j < numLeft; j++) {
            int vertexId = vertexIdTranslate.forward(j);  // Translate to internal vertex id
            for(int i=0; i < params.D; i++) {
                wr.write(params.latentFactors.getValue(vertexId, i) + "\n");
            }
        }
        wr.close();

         Output right 
        String rightFileName = this.problemSetup.training + "_V.mm";
        wr = new BufferedWriter(new FileWriter(rightFileName));
        wr.write("%%MatrixMarket matrix array real general\n");
        wr.write(params.D + " " + numRight + "\n");

        for(int j=0; j < numRight; j++) {
            int vertexId = vertexIdTranslate.forward(numLeft + j);   // Translate to internal vertex id
            for(int i=0; i < params.D; i++) {
                wr.write(params.latentFactors.getValue(vertexId, i) + "\n");
            }
        }
        wr.close();

        this.logger.info("Latent factor matrices saved: " + 
        		this.problemSetup.training + "_U.mm" + ", " + this.problemSetup.training + "_V.mm");
    }*/
}
