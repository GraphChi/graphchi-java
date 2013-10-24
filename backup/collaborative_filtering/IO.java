package edu.cmu.graphchi.toolkits.collaborative_filtering;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;

public class IO {

	public static void convert_matrix_market() throws IOException{
		  /* Run sharding (preprocessing) if the files do not exist yet */
        FastSharder sharder = createSharder(ProblemSetup.training, ProblemSetup.nShards);
        if (!new File(ChiFilenames.getFilenameIntervals(ProblemSetup.training, ProblemSetup.nShards)).exists() ||
                !new File(ProblemSetup.training + ".matrixinfo").exists()) {
            sharder.shard(new FileInputStream(new File(ProblemSetup.training)), FastSharder.GraphInputFormat.MATRIXMARKET);
        } else {
            ProblemSetup.logger.info("Found shards -- no need to preprocess");
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
    
}
