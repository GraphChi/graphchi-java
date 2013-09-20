package edu.cmu.graphchi.toolkits.collaborative_filtering;

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

	public static void convert_matrix_market(ProblemSetup problemSetup) throws IOException{
		  /* Run sharding (preprocessing) if the files do not exist yet */
        FastSharder sharder = createSharder(problemSetup.training, problemSetup.nShards);
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
    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Integer, Float>(graphName, numShards, null, new EdgeProcessor<Float>() {
            public Float receiveEdge(int from, int to, String token) {
                return (token == null ? 0.0f : Float.parseFloat(token));
            }
        }, new IntConverter(), new FloatConverter());
    }

    /**
     * Initialize the sharder-program. This sharder program will also record certain metadata
     * like global mean to an info file.
     * @param graphName
     * @param numShards
     * @return
     * @throws java.io.IOException
     */
	public static void convert_matrix_market_metadata(ProblemSetup problemSetup) throws IOException{
		  /* Run sharding (preprocessing) if the files do not exist yet */
		GlobalMeanEdgeProcessor edgeProcessor = new GlobalMeanEdgeProcessor(); 
		FastSharder sharder =  new FastSharder<Integer, Float>(problemSetup.training, 
				problemSetup.nShards, null, edgeProcessor, new IntConverter(), new FloatConverter());
		
      if (!new File(ChiFilenames.getFilenameIntervals(problemSetup.training, problemSetup.nShards)).exists() ||
              !new File(problemSetup.training + ".matrixinfo").exists()) {
          sharder.shard(new FileInputStream(new File(problemSetup.training)), FastSharder.GraphInputFormat.MATRIXMARKET);
          sharder.addMetadata("globalMean", edgeProcessor.getGlobalMean()+"");
        //Write the metadata map which was populated during the sharding process.
         sharder.writeMetadata();
      } else {
          //problemSetup.logger.info("Found shards -- no need to preprocess");
      }
      
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
