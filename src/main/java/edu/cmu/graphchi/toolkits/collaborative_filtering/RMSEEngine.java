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
import java.util.logging.Logger;
/*
 * @author  Danny Bickson, CMU, 2013
 */
public class RMSEEngine extends ProblemSetup implements GraphChiProgram<Integer, Float>{

  
	double validation_rmse = 0;
	
    public RMSEEngine() {
    }
    
    @Override
    public void update(ChiVertex<Integer, Float> vertex, GraphChiContext context) {
        if (vertex.numOutEdges() == 0) return;

        RealVector latent_factor = ProblemSetup.latent_factors_inmem.getRowAsVector(vertex.getId());
        try {
            double squaredError = 0;
            for(int e=0; e < vertex.numEdges(); e++) {
                float observation = vertex.edge(e).getValue();
                RealVector neighbor = ProblemSetup.latent_factors_inmem.getRowAsVector(vertex.edge(e).getVertexId());
                double prediction = ALS.als_predict(neighbor, latent_factor);
                squaredError += Math.pow(prediction - observation,2);    
            }
 
            synchronized (this) {
                 validation_rmse += squaredError;
            }    
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
    	validation_rmse = 0;
    }

    @Override
    public void endIteration(GraphChiContext ctx) {
    	 /* Output RMSE */
        double validationRMSE = Math.sqrt(validation_rmse / (1.0 * 545000 /*ctx.getNumEdges()*/));
        logger.info("Training RMSE: " + String.format("%.5f", ProblemSetup.train_rmse) + " Validation RMSE: " + String.format("%.5f", validationRMSE));
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

    public void calc_validation_rmse(String baseFilename, int nShards){
    	/* Run GraphChi */
    	try{
        GraphChiEngine<Integer, Float> engine = new GraphChiEngine<Integer, Float>(baseFilename + "e", nShards);
        engine.setEdataConverter(new FloatConverter());
        engine.setEnableDeterministicExecution(false);
        engine.setVertexDataConverter(null);  // We do not access vertex values.
        engine.setModifiesInedges(false); // Important optimization
        engine.setModifiesOutedges(false); // Important optimization
        
        engine.run(this, 1);
        } catch (Exception ex){
        	logger.info("Failed to compute validation rmse");
        }
    }
    
    void init_validation() {	
    	try {
    /* Run sharding (preprocessing) if the files do not exist yet */
    sharder_validation = IO.createSharder(training + "e", 1);
    if (!new File(ChiFilenames.getFilenameIntervals(training + "e", nShards)).exists() ||
            !new File(training + "e.matrixinfo").exists()) {
        sharder_validation.shard(new FileInputStream(new File(training + "e")), FastSharder.GraphInputFormat.MATRIXMARKET);
    } else {
        logger.info("Found validation shards -- no need to preprocess");
    }
    	} catch (IOException ex){
    	   logger.warning("Failed to initalize validation input. Aborting.");	
    	}
    }

    
}