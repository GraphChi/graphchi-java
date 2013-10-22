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

	public static void convertMatrixMarket(ProblemSetup problemSetup) throws IOException{
		  /* Run sharding (preprocessing) if the files do not exist yet */
        FastSharder sharder = createSharder(problemSetup.training, problemSetup.nShards);
        if (!new File(ChiFilenames.getFilenameIntervals(problemSetup.training, problemSetup.nShards)).exists() ||
                !new File(problemSetup.training + ".matrixinfo").exists()) {
            sharder.shard(new FileInputStream(new File(problemSetup.training)), FastSharder.GraphInputFormat.MATRIXMARKET);
        } else {
            //problemSetup.logger.info("Found shards -- no need to preprocess");
        }
	}
	
	public static void convertMatrixMarket(ProblemSetup problemSetup, FastSharder sharder) 
			throws IOException{
		  /* Run sharding (preprocessing) if the files do not exist yet */
      if (!new File(ChiFilenames.getFilenameIntervals(problemSetup.training, problemSetup.nShards)).exists() ||
              !new File(problemSetup.training + ".matrixinfo").exists()) {
          sharder.shard(new FileInputStream(new File(problemSetup.training)), FastSharder.GraphInputFormat.MATRIXMARKET);
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

    static class GlobalMeanEdgeProcessor implements EdgeProcessor<Float> {
    	private long globalSum;
    	private long count;
		@Override
		public Float receiveEdge(int from, int to, String token) {
			float edgeVal = (token == null ? 0.0f : Float.parseFloat(token));
			globalSum += edgeVal;
			count++;
			return edgeVal;
		}
    	
		public double getGlobalMean() {
			return ((double) globalSum)/count;
		}
    }
    
    
}
