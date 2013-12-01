package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.RatingEdge;

public interface RecommenderAlgorithm extends GraphChiProgram<Integer, RatingEdge> {

    public boolean hasConverged(GraphChiContext ctx);

    public ModelParameters getParams();
	
	public DataSetDescription getDataSetDescription();
	
	public String getSerializedOutputLoc();
	
	/**
	 * Should return how much memory this particular recommender is estimated to consume
	 * in MBs.
	 * @return
	 */
	public int getEstimatedMemoryUsage();
	
}
