package edu.cmu.graphchi.apps.pig;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.hadoop.PigGraphChiBase;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.util.FileUtils;
import edu.cmu.graphchi.util.HugeDoubleMatrix;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import org.apache.commons.math.linear.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.*;
import java.util.logging.Logger;

/**
 * <b>Version for Pig http://pig.apache.org</b>
 *
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
 * This application expects an edge list input, for example with Netflix
 * movie ratings data:
 *    user-id movie-id rating
 *    ...
 *
 * <b>Bipartite graph handling: </b> The problem is represented as bipartite
 * graph with "users" on the left side and "movies" on the right side (using
 * the movie rating case as an example). Users have directed edge towards movies.
 * However, when represented as a graph in GraphChi, both left and right
 * side start vertex id numbers from zero. Thus, vertex id A might be both
 * a movie and an user. (Perhaps in the future GraphChi has proper special
 * support for bipartite graphs). Thus, each vertex is handled separately as
 * a movie, and as a user.
 *
 * Each vertex stores a factor, which is kept in memory.
 *
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, 2013
 */
public class PigALSMatrixFactorization extends PigGraphChiBase
        implements GraphChiProgram<Integer, Float> {

    private static Logger logger = ChiLogger.getLogger("ALS");

    /* Used for storing the vertex values in memory efficiently. */
    private HugeDoubleMatrix leftSideMatrix;
    private HugeDoubleMatrix rightSideMatrix;

    private int D=5;

    double LAMBDA = 0.065;
    double rmse = 0.0;

    public PigALSMatrixFactorization() {
    }


    /* This is a bipartite graph. */
    private final static int LEFTSIDE = 0; // Start with right side
    private final static int RIGHTSIDE = 1;

    /* We keep track of these while sharding the graph */
    private int maxLeftVertexId = 0;
    private int maxRightVertexId = 0;

    @Override
    public void update(ChiVertex<Integer, Float> vertex, GraphChiContext context) {
        if (vertex.numEdges() == 0) return;


        VertexIdTranslate idTranslate = context.getVertexIdTranslate();

        for(int side=LEFTSIDE; side<=RIGHTSIDE; side++) {
            /* The latent factors for both sides of the graph are kept in memory,
               but in separate matrices. This chooses which one matrix has the value
               of the vertex in question, and which has neighbors.
             */
            HugeDoubleMatrix thisSideMatrix = (side == LEFTSIDE ? leftSideMatrix : rightSideMatrix);
            HugeDoubleMatrix otherSideMatrix = (side == LEFTSIDE ? rightSideMatrix : leftSideMatrix);

            /* Check if this vertex is active on the given side (left or right) */
            if (side == LEFTSIDE && vertex.numOutEdges() == 0) continue;
            if (side == RIGHTSIDE && vertex.numInEdges() == 0) continue;

            /* Start computing the new factor */
            RealMatrix XtX = new BlockRealMatrix(D, D);
            RealVector Xty = new ArrayRealVector(D);

            try {
                double[] neighborLatent = new double[D];

                int ne = (side == LEFTSIDE ? vertex.numOutEdges() : vertex.numInEdges());
                // Compute XtX and Xty (NOTE: unweighted)
                for(int e=0; e < ne; e++) {
                    ChiEdge<Float> edge = (side == LEFTSIDE ? vertex.outEdge(e) : vertex.inEdge(e));
                    float observation = edge.getValue();
                    if (observation < 1.0) throw new RuntimeException("Had invalid observation: " + observation + " on edge " + idTranslate.backward(vertex.getId()) + "->" +
                                idTranslate.backward(edge.getVertexId()));
                    otherSideMatrix.getRow(idTranslate.backward(edge.getVertexId()), neighborLatent);

                    for(int i=0; i < D; i++) {
                        Xty.setEntry(i, Xty.getEntry(i) + neighborLatent[i] * observation);
                        for(int j=i; j < D; j++) {
                            XtX.setEntry(j,i, XtX.getEntry(j, i) + neighborLatent[i] * neighborLatent[j]);
                        }
                    }
                }

                // Symmetrize
                for(int i=0; i < D; i++) {
                    for(int j=i+1; j< D; j++) XtX.setEntry(i,j, XtX.getEntry(j, i));
                }

                // Diagonal -- add regularization
                for(int i=0; i < D; i++) XtX.setEntry(i, i, XtX.getEntry(i, i) + LAMBDA * vertex.numEdges());

                // Solve the least-squares optimization using Cholesky Decomposition
                RealVector newLatentFactor = new CholeskyDecompositionImpl(XtX).getSolver().solve(Xty);

                // Set the new latent factor for this vector
                for(int i=0; i < D; i++) {
                    thisSideMatrix.setValue(idTranslate.backward(vertex.getId()), i, newLatentFactor.getEntry(i));
                }

                if (context.isLastIteration() && side == RIGHTSIDE) {
                    /* On the last iteration - compute the RMSE error. But only for
                      vertices on the right side of the matrix, i.e vectors
                      that have only in-edges.
                    */
                    if (vertex.numInEdges() > 0) {
                        // Sanity check
                        double squaredError = 0;
                        for(int e=0; e < vertex.numInEdges(); e++) {
                            // Compute RMSE
                            ChiEdge<Float> edge = vertex.inEdge(e);
                            float observation = edge.getValue();
                            otherSideMatrix.getRow(idTranslate.backward(edge.getVertexId()), neighborLatent);
                            double prediction = new ArrayRealVector(neighborLatent).dotProduct(newLatentFactor);
                            squaredError += (prediction - observation) * (prediction - observation);
                        }

                        synchronized (this) {
                            rmse += squaredError;
                        }
                    }
                }

            } catch (NotPositiveDefiniteMatrixException npdme) {
                logger.warning("Matrix was not positive definite: " + XtX);
            } catch (Exception err) {
                err.printStackTrace();
                throw new RuntimeException(err);
            }
        }
    }

    @Override
    public void beginIteration(GraphChiContext ctx) {
        /* On first iteration, initialize the vertices in memory.
         * Vertices' latent factors are stored in the vertexValueMatrix
         * so that each row contains one latent factor.
         */
        if (ctx.getIteration() == 0) {
            logger.info("Initializing latent factors for " + (1 + maxLeftVertexId) + " vertices on the left side");
            logger.info("Initializing latent factors for " + (1 + maxRightVertexId) + " vertices on the right side");

            leftSideMatrix = new HugeDoubleMatrix(maxLeftVertexId + 1, D);
            rightSideMatrix = new HugeDoubleMatrix(maxRightVertexId + 1, D);
            /* Fill with random data */
            leftSideMatrix.randomize(0f, 1.0f);
            rightSideMatrix.randomize(0f, 1.0f);
        }
    }

    @Override
    public void endIteration(GraphChiContext ctx) {
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
     * Initialize the sharder-program.
     * @param graphName
     * @param numShards
     * @return
     * @throws java.io.IOException
     */
    protected FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Integer, Float>(graphName, numShards, null
                , new EdgeProcessor<Float>() {
            public Float receiveEdge(int from, int to, String token) {
                /* Keep track of the graph dimension*/
                maxLeftVertexId = Math.max(from, maxLeftVertexId);
                maxRightVertexId = Math.max(to, maxRightVertexId);
                return (token == null ? 0.0f : Float.parseFloat(token));
            }
        }, new IntConverter(), new FloatConverter());
    }

    @Override
    protected String getSchemaString() {
        String s = "{factor:string,id:int";
        for(int i=0; i<D; i++) s += ",x" + i;
        s += "}";
        return s;
    }

    @Override
    protected int getNumShards() {
        return 20;
    }


    private int outputCounter = 0;

    @Override
    protected void runGraphChi() throws Exception {
        /* Run GraphChi */
        GraphChiEngine<Integer, Float> engine = new GraphChiEngine<Integer, Float>(getGraphName(), getNumShards());
        engine.setEdataConverter(new FloatConverter());
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        engine.setModifiesOutedges(false); // Important optimization

        /* Run for 5 iterations */
        engine.run(this, 5);

        /* Output RMSE */
        double trainRMSE = Math.sqrt(this.rmse / (1.0 * engine.numEdges()));
        logger.info("Train RMSE: " + trainRMSE + ", total edges:" + engine.numEdges());
    }

    @Override
    protected Tuple getNextResult(TupleFactory tupleFactory) throws ExecException {
        HugeDoubleMatrix matrix;
        int vertexId = 0;
        String factor;
        if (outputCounter < maxLeftVertexId) {
            matrix = leftSideMatrix;
            vertexId = outputCounter;
            factor = "U";
        } else {
            matrix = rightSideMatrix;
            vertexId = outputCounter - maxLeftVertexId;
            factor = "V";
            if (vertexId >= rightSideMatrix.getNumRows()) return null;
        }
        Tuple t = tupleFactory.newTuple(2 + D);
        t.set(0, factor);
        t.set(1, vertexId);
        for(int i=0; i<D; i++) {
            t.set(2 + i, matrix.getValue(vertexId, i));
        }
        outputCounter++;
        return t;

    }
}
