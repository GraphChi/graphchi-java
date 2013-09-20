package edu.cmu.graphchi.toolkits.collaborative_filtering;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.util.FileUtils;
import edu.cmu.graphchi.util.HugeDoubleMatrix;
import org.apache.commons.math.linear.*;

import java.io.*;
import java.util.Map;
import java.util.logging.Logger;

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
 */
public class ALS implements GraphChiProgram<Integer, Float>{
    
	double LAMBDA = 0.065;
	private ProblemSetup problemSetup;
	private HugeDoubleMatrix latent_factors_inmem;
	protected Logger logger = ChiLogger.getLogger("ALS");
    double train_rmse = 0.0;

    public ALS(ProblemSetup problemSetup) {
    	this.problemSetup = problemSetup;
    }

    public double als_predict(RealVector user, RealVector item){
    	double prediction = user.dotProduct(item);
    	prediction = Math.min(prediction, this.problemSetup.maxval);
    	prediction = Math.max(prediction, this.problemSetup.minval);
    	return prediction;
    }
    
    void init_feature_vectors(long size){
        this.logger.info("Initializing latent factors for " + size + " vertices");
        latent_factors_inmem = new HugeDoubleMatrix(size, this.problemSetup.D);

        /* Fill with random data */
        latent_factors_inmem.randomize(0f, 1.0f);
      }
    
    
    @Override
    public void update(ChiVertex<Integer, Float> vertex, GraphChiContext context) {
        if (vertex.numEdges() == 0) return;

        RealMatrix XtX = new BlockRealMatrix(this.problemSetup.D, this.problemSetup.D);
        RealVector Xty = new ArrayRealVector(this.problemSetup.D);
        RealVector latent_factor = latent_factors_inmem.getRowAsVector(vertex.getId());
     

        try {
            double squaredError = 0;
            boolean is_user = vertex.numOutEdges() > 0;
            // Compute XtX and Xty (NOTE: unweighted)
            for(int e=0; e < vertex.numEdges(); e++) {
                float observation = vertex.edge(e).getValue();
                RealVector neighbor = latent_factors_inmem.getRowAsVector(vertex.edge(e).getVertexId());
            
                //Xty.add(neighbor * observation);
                //XtX.add(neighbor.outerProduct(neighbor));
                for(int i=0; i < this.problemSetup.D; i++) {
                    Xty.setEntry(i, Xty.getEntry(i) + neighbor.getEntry(i) * observation);
                    for(int j=i; j < this.problemSetup.D; j++) {
                        XtX.setEntry(j,i, XtX.getEntry(j, i) + neighbor.getEntry(i) * neighbor.getEntry(j));
                    }
                }
                
                // Symmetrize
                for(int i=0; i < this.problemSetup.D; i++) {
                    for(int j=i+1; j< this.problemSetup.D; j++) XtX.setEntry(i,j, XtX.getEntry(j, i));
                }
                
                if (is_user){
                  double prediction = als_predict(neighbor, new ArrayRealVector(latent_factor));
                  squaredError += Math.pow(prediction - observation,2);
                }
            }
            
            // Diagonal -- add regularization
            for (int i=0; i < this.problemSetup.D; i++) 
            	XtX.setEntry(i, i, XtX.getEntry(i, i) + LAMBDA * vertex.numEdges());

            // Solve the least-squares optimization using Cholesky Decomposition
            RealVector newLatentFactor = new CholeskyDecompositionImpl(XtX).getSolver().solve(Xty);
            latent_factors_inmem.setRow(vertex.getId(), newLatentFactor.getData());

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
        	   init_feature_vectors(ctx.getNumVertices());
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
    	
        ALS als = new ALS(problemSetup);
        als.logger.info("Set latent factor dimension to: " + problemSetup.D);

        IO.convert_matrix_market(problemSetup);
        
        /* Run GraphChi */
        GraphChiEngine<Integer, Float> engine = new GraphChiEngine<Integer, Float>(problemSetup.training, problemSetup.nShards);
        
        engine.setEdataConverter(new FloatConverter());
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        engine.setModifiesOutedges(false); // Important optimization
        engine.run(als, 5);

        als.writeOutputMatrices(engine.getVertexIdTranslate());
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
        wr.write(this.problemSetup.D + " " + numLeft + "\n");

        for(int j=0; j < numLeft; j++) {
            int vertexId = vertexIdTranslate.forward(j);  // Translate to internal vertex id
            for(int i=0; i < this.problemSetup.D; i++) {
                wr.write(latent_factors_inmem.getValue(vertexId, i) + "\n");
            }
        }
        wr.close();

        /* Output right */
        String rightFileName = this.problemSetup.training + "_V.mm";
        wr = new BufferedWriter(new FileWriter(rightFileName));
        wr.write("%%MatrixMarket matrix array real general\n");
        wr.write(this.problemSetup.D + " " + numRight + "\n");

        for(int j=0; j < numRight; j++) {
            int vertexId = vertexIdTranslate.forward(numLeft + j);   // Translate to internal vertex id
            for(int i=0; i < this.problemSetup.D; i++) {
                wr.write(latent_factors_inmem.getValue(vertexId, i) + "\n");
            }
        }
        wr.close();

        this.logger.info("Latent factor matrices saved: " + 
        		this.problemSetup.training + "_U.mm" + ", " + this.problemSetup.training + "_V.mm");
    }
}
