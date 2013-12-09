package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.RatingEdge;

/**
 * Represents the generic recommender algorithm class. All recommender algorithms implement
 * this interface. 
 * @author mayank
 */

public interface RecommenderAlgorithm extends GraphChiProgram<Integer, RatingEdge> {

    /**
     * should return true if the recommender algorithm has converged. 
     * @param ctx
     * @return
     */
    public boolean hasConverged(GraphChiContext ctx);

    /**
     * Returns the ModelParameter object of the reccommender
     * @return
     */
    public ModelParameters getParams();
	
    /**
     * Returns the DataSetDescription which this recommender algorithm is initialized with.
     * @return
     */
	public DataSetDescription getDataSetDescription();
	
	/**
	 * Returns the URI (for example path for local FS or HDFS location) where the 
	 * serialized model after training / checkpointing is stored.
	 * @return
	 */
	public String getSerializedOutputLoc();
	
	/**
	 * Should return how much memory this particular recommender is estimated to consume
	 * in MBs.
	 * @return
	 */
	public int getEstimatedMemoryUsage();
	
}
