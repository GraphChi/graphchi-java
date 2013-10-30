package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;

public interface RecommenderAlgorithm extends GraphChiProgram<Integer, RatingEdge> {

	public ModelParameters getParams();
	
	public boolean hasConverged(GraphChiContext ctx);
	
	public DataSetDescription getDataSetDescription();
	
}
