package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.BlockRealMatrix;
import org.apache.commons.math.linear.CholeskyDecompositionImpl;
import org.apache.commons.math.linear.NotPositiveDefiniteMatrixException;
import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.linear.RealVector;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.util.HugeDoubleMatrix;

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
 * This application reads Matrix Market format (similar to the C++ version)
 * and outputs the latent factors in two files in the matrix market format.
 * To test, you can download small-netflix data from here:
 * http://select.cs.cmu.edu/code/graphlab/smallnetflix_mme
 *
 * <i>Note:</i>  in this case the vertex values are not used, but as GraphChi does
 * not currently support "no-vertex-values", integer-type is used as placeholder.
 *
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, 2013
 * @author Modifications by Danny Bickson, CMU, 2013
 * @author Modifications by Mayank Mohta (mmohta@andrew.cmu.edu), CMU, 2013
 */

class ALSParams extends ModelParameters {
	double LAMBDA;	//regularization
	int D;	//number of features
	HugeDoubleMatrix latentFactors;
	
	public ALSParams(String id, String json) {
		super(id, json);
		
		setDefaults();
		
		parseParameters(json);
		
	}
	
	public void setDefaults() {
		this.LAMBDA = 0.065;
		this.D = 10;
	}
	
	public void parseParameters(String json) {
		
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
	
}

public class ALS implements GraphChiProgram<Integer, Float>{
    
	double LAMBDA = 0.065;
	private ProblemSetup problemSetup;
	private ALSParams params;
	protected Logger logger = ChiLogger.getLogger("ALS");
    double train_rmse = 0.0;

    public ALS(ProblemSetup problemSetup, ModelParameters params) {
    	this.problemSetup = problemSetup;
    	this.params = (ALSParams)params;
    }

    public double als_predict(RealVector user, RealVector item){
    	double prediction = user.dotProduct(item);
    	prediction = Math.min(prediction, this.problemSetup.maxval);
    	prediction = Math.max(prediction, this.problemSetup.minval);
    	return prediction;
    }
    
    
    @Override
    public void update(ChiVertex<Integer, Float> vertex, GraphChiContext context) {
        if (vertex.numEdges() == 0) return;

        RealMatrix XtX = new BlockRealMatrix(params.D, params.D);
        RealVector Xty = new ArrayRealVector(params.D);
        RealVector latent_factor = params.latentFactors.getRowAsVector(vertex.getId());
     

        try {
            double squaredError = 0;
            boolean is_user = vertex.numOutEdges() > 0;
            // Compute XtX and Xty (NOTE: unweighted)
            for(int e=0; e < vertex.numEdges(); e++) {
                float observation = vertex.edge(e).getValue();
                RealVector neighbor = params.latentFactors.getRowAsVector(vertex.edge(e).getVertexId());
            
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
                  double prediction = als_predict(neighbor, new ArrayRealVector(latent_factor));
                  squaredError += Math.pow(prediction - observation,2);
                }
            }
            
            // Diagonal -- add regularization
            for (int i=0; i < params.D; i++) 
            	XtX.setEntry(i, i, XtX.getEntry(i, i) + LAMBDA * vertex.numEdges());

            // Solve the least-squares optimization using Cholesky Decomposition
            RealVector newLatentFactor = new CholeskyDecompositionImpl(XtX).getSolver().solve(Xty);
            params.latentFactors.setRow(vertex.getId(), newLatentFactor.getData());

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

    /**
     * Usage: java edu.cmu.graphchi.ALSMatrixFactorization <input-file> <nshards> <D>
     * Normally nshards of 10 or so is fine.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

    	ProblemSetup problemSetup = new ProblemSetup(args);
    	ModelParameters params = new ALSParams(problemSetup.getRunId("ALS"), problemSetup.paramJson);
    	
        ALS als = new ALS(problemSetup, params);

        IO.convertMatrixMarket(problemSetup);
        
        /* Run GraphChi */
        GraphChiEngine<Integer, Float> engine = new GraphChiEngine<Integer, Float>(problemSetup.training, problemSetup.nShards);
        
        engine.setEdataConverter(new FloatConverter());
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        engine.setModifiesOutedges(false); // Important optimization
        engine.run(als, 5);

        params.serialize(problemSetup.outputLoc);
    }

    /**
     * Output in matrix market format
     * @param training
     * @param vertexIdTranslate
     * @throws Exception
     */
    private void writeOutputMatrices(VertexIdTranslate vertexIdTranslate) throws Exception {
    	Map<String, String> metaDataMap = FastSharder.readMetadata(
    			ChiFilenames.getFilenameMetadata(this.problemSetup.training, this.problemSetup.nShards));
    	int numLeft = Integer.parseInt(metaDataMap.get("numLeft"));
    	int numRight = Integer.parseInt(metaDataMap.get("numRight"));
    	
        /* Output left */
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

        /* Output right */
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
    }
}
