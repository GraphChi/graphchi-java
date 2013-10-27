package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;

public class IO {

	public static void convertMatrixMarket(String trainingFile, int nshards) throws IOException{
		  /* Run sharding (preprocessing) if the files do not exist yet */
        FastSharder sharder = createSharder(trainingFile, nshards);
        if (!new File(ChiFilenames.getFilenameIntervals(trainingFile, nshards)).exists() ||
                !new File(trainingFile + ".matrixinfo").exists()) {
            sharder.shard(new FileInputStream(new File(trainingFile)), FastSharder.GraphInputFormat.MATRIXMARKET);
        } else {
            //problemSetup.logger.info("Found shards -- no need to preprocess");
        }
	}
	
	public static void convertMatrixMarket(String trainingFile, int nshards, FastSharder sharder) 
			throws IOException{
		  /* Run sharding (preprocessing) if the files do not exist yet */
      if (!new File(ChiFilenames.getFilenameIntervals(trainingFile, nshards)).exists() ||
              !new File(trainingFile + ".matrixinfo").exists()) {
          sharder.shard(new FileInputStream(new File(trainingFile)), FastSharder.GraphInputFormat.MATRIXMARKET);
      } else {
          //problemSetup.logger.info("Found shards -- no need to preprocess");
      }
	}
	
	 /**
     * Initialize the sharder-program.
     * @param graphName
     * @param numShards
     * @return
     * @throws java.io.IOException
     */
    public static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Integer, Float>(graphName, numShards, null, new EdgeProcessor<Float>() {
            public Float receiveEdge(int from, int to, String token) {
                return (token == null ? 0.0f : Float.parseFloat(token));
            }
        }, new IntConverter(), new FloatConverter());
    }
    
}
