package edu.cmu.graphchi.apps;

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
 */
public class ALSMatrixFactorization implements GraphChiProgram<Integer, Float> {

    protected static Logger logger = ChiLogger.getLogger("ALS");

    /* Used for storing the vertex values in memory efficiently. */
    protected HugeDoubleMatrix vertexValueMatrix;
    protected int D;

    protected String baseFilename;
    protected int numShards;

    protected double LAMBDA = 0.065;
    protected double rmse = 0.0;

    protected ALSMatrixFactorization(int D, String baseFilename, int numShards) {
        this.D = D; // Dimensionality factor
        this.numShards = numShards;
        this.baseFilename = baseFilename;
    }

    /**
     * Compute a prediction. Note: need to use the internal vertex-ids.
     * @param leftVertex (often "user")
     * @param rightVertex (often "movie")
     * @return
     */
    public double predict(int leftVertex, int rightVertex) {
        double[] leftLatent = new double[D];
        double[] rightLatent = new double[D];
        vertexValueMatrix.getRow(leftVertex, leftLatent);
        vertexValueMatrix.getRow(rightVertex, rightLatent);
        return new ArrayRealVector(leftLatent).dotProduct(new ArrayRealVector(rightLatent));
    }

    @Override
    public void update(ChiVertex<Integer, Float> vertex, GraphChiContext context) {
        if (vertex.numEdges() == 0) return;

        RealMatrix XtX = new BlockRealMatrix(D, D);
        RealVector Xty = new ArrayRealVector(D);

        try {
            double[] neighborLatent = new double[D];

            // Compute XtX and Xty (NOTE: unweighted)
            for(int e=0; e < vertex.numEdges(); e++) {
                ChiEdge<Float> edge = vertex.edge(e);
                float observation = edge.getValue();
                vertexValueMatrix.getRow(edge.getVertexId(), neighborLatent);

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
                vertexValueMatrix.setValue(vertex.getId(), i, newLatentFactor.getEntry(i));
            }

            if (context.isLastIteration()) {
                /* On the last iteration - compute the RMSE error. But only for
                   vertices on the right side of the matrix, i.e vectors
                   that have only in-edges.
                 */
                if (vertex.numInEdges() > 0) {
                    // Sanity check
                    if (vertex.numOutEdges() > 0) throw new IllegalStateException("Not a bipartite graph!");
                    double squaredError = 0;
                    for(int e=0; e < vertex.numInEdges(); e++) {
                        // Compute RMSE
                        ChiEdge<Float> edge = vertex.edge(e);
                        float observation = edge.getValue();
                        vertexValueMatrix.getRow(edge.getVertexId(), neighborLatent);
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
            throw new RuntimeException(err);
        }
    }

    @Override
    public void beginIteration(GraphChiContext ctx) {
        /* On first iteration, initialize the vertices in memory.
         * Vertices' latent factors are stored in the vertexValueMatrix
         * so that each row contains one latent factor.
         */
        if (ctx.getIteration() == 0) {
            logger.info("Initializing latent factors for " + ctx.getNumVertices() + " vertices");
            vertexValueMatrix = new HugeDoubleMatrix(ctx.getNumVertices(), D);

            /* Fill with random data */
            vertexValueMatrix.randomize(0f, 1.0f);
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
    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Integer, Float>(graphName, numShards, null, new EdgeProcessor<Float>() {
            public Float receiveEdge(int from, int to, String token) {
                return (token == null ? 0.0f : Float.parseFloat(token));
            }
        }, new IntConverter(), new FloatConverter());
    }


    /**
     * Usage: java edu.cmu.graphchi.ALSMatrixFactorization <input-file> <nshards> <D>
     * Normally nshards of 10 or so is fine.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: java edu.cmu.graphchi.ALSMatrixFactorization <input-file> <nshards> <D>");
        }
        String baseFilename = args[0];
        int nShards = Integer.parseInt(args[1]);
        int D = 20;
        if (args.length == 3) {
            D = Integer.parseInt(args[2]);
        }
        ALSMatrixFactorization als = computeALS(baseFilename, nShards, D, 5);


        als.writeOutputMatrices();
    }


    /**
     * Compute ALS and return the ALS object which can be used to
     * compute predictions.
     * @param baseFilename
     * @param nShards
     * @param D
     * @return
     * @throws IOException
     */
    public static ALSMatrixFactorization computeALS(String baseFilename, int nShards, int D, int iterations) throws IOException {
        /* Run sharding (preprocessing) if the files do not exist yet */
        FastSharder sharder = createSharder(baseFilename, nShards);
        if (!new File(ChiFilenames.getFilenameIntervals(baseFilename, nShards)).exists() ||
                !new File(baseFilename + ".matrixinfo").exists()) {
            sharder.shard(new FileInputStream(new File(baseFilename)), FastSharder.GraphInputFormat.MATRIXMARKET);
        } else {
            logger.info("Found shards -- no need to preprocess");
        }

        /* Init */
        ALSMatrixFactorization als = new ALSMatrixFactorization(D, baseFilename, nShards);
        logger.info("Set latent factor dimension to: " + als.D);

        /* Run GraphChi */
        GraphChiEngine<Integer, Float> engine = new GraphChiEngine<Integer, Float>(baseFilename, nShards);
        engine.setEdataConverter(new FloatConverter());
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        engine.setModifiesOutedges(false); // Important optimization

        engine.run(als, iterations);

        /* Output RMSE */
        double trainRMSE = Math.sqrt(als.rmse / (1.0 * engine.numEdges()));
        logger.info("Train RMSE: " + trainRMSE + ", total edges:" + engine.numEdges());
        return als;
    }

    public BipartiteGraphInfo getGraphInfo() {
        String infoFile = baseFilename + ".matrixinfo";
        try {
            String info = FileUtils.readToString(infoFile);
            String[] tok = info.split("\t");
            int numLeft = Integer.parseInt(tok[0]);
            int numRight = Integer.parseInt(tok[1]);
            return new BipartiteGraphInfo(numLeft, numRight);
        } catch (IOException ioe) {
            throw new RuntimeException("Could not load matrix info! File: " + infoFile);
        }
    }

    public class BipartiteGraphInfo {
        private int numLeft;
        private int numRight;

        public BipartiteGraphInfo(int numLeft, int numRight) {
            this.numLeft = numLeft;
            this.numRight = numRight;
        }

        public int getNumLeft() {
            return numLeft;
        }

        public int getNumRight() {
            return numRight;
        }
    }

    /**
     * Output in matrix market format
     * @throws Exception
     */
    private void writeOutputMatrices() throws Exception {
        /* First read the original matrix dimensions */
        BipartiteGraphInfo graphInfo = getGraphInfo();
        int numLeft = graphInfo.getNumLeft();
        int numRight = graphInfo.getNumRight();

        VertexIdTranslate vertexIdTranslate =
                VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards)));

        /* Output left */
        String leftFileName = baseFilename + "_U.mm";
        BufferedWriter wr = new BufferedWriter(new FileWriter(leftFileName));
        wr.write("%%MatrixMarket matrix array real general\n");
        wr.write(this.D + " " + numLeft + "\n");

        for(int j=0; j < numLeft; j++) {
            int vertexId = vertexIdTranslate.forward(j);  // Translate to internal vertex id
            for(int i=0; i < D; i++) {
                wr.write(vertexValueMatrix.getValue(vertexId, i) + "\n");
            }
        }
        wr.close();

        /* Output right */
        String rightFileName = baseFilename + "_V.mm";
        wr = new BufferedWriter(new FileWriter(rightFileName));
        wr.write("%%MatrixMarket matrix array real general\n");
        wr.write(this.D + " " + numRight + "\n");

        for(int j=0; j < numRight; j++) {
            int vertexId = vertexIdTranslate.forward(numLeft + j);   // Translate to internal vertex id
            for(int i=0; i < D; i++) {
                wr.write(vertexValueMatrix.getValue(vertexId, i) + "\n");
            }
        }
        wr.close();

        logger.info("Latent factor matrices saved: " + baseFilename + "_U.mm" + ", " + baseFilename + "_V.mm");
    }
}
