package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;

public class AggregateRecommender implements
		GraphChiProgram<Integer, RatingEdge> {
	
	List<GraphChiProgram> recommenders;
	
	public AggregateRecommender(DataSetDescription datasetDesc, String modelDescJsonFile) {
		this.recommenders = RecommenderFactory.buildRecommenders(datasetDesc, modelDescJsonFile);
	}

	@Override
	public void update(ChiVertex<Integer, RatingEdge> vertex,
			GraphChiContext context) {
		for(GraphChiProgram rec : this.recommenders) {
			rec.update(vertex, context);
		}
	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		for(GraphChiProgram rec : this.recommenders) {
			rec.beginIteration(ctx);
		}
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		for(GraphChiProgram rec : this.recommenders) {
			rec.endIteration(ctx);
		}
	}

	@Override
	public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
		for(GraphChiProgram rec : this.recommenders) {
			rec.beginInterval(ctx, interval);
		}
	}

	@Override
	public void endInterval(GraphChiContext ctx, VertexInterval interval) {
		for(GraphChiProgram rec : this.recommenders) {
			rec.endInterval(ctx, interval);
		}
		
	}

	@Override
	public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
		for(GraphChiProgram rec : this.recommenders) {
			rec.beginSubInterval(ctx, interval);
		}
	}

	@Override
	public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
		for(GraphChiProgram rec : this.recommenders) {
			rec.endSubInterval(ctx, interval);
		}		
	}

	protected static FastSharder createSharder(String graphName, int numShards, int num_edge_features) throws IOException {
        return new FastSharder<Integer, RatingEdge>(graphName, numShards, null, 
        		new RatingEdgeProcessor(), 
        	new IntConverter(), new RatingEdgeConvertor(num_edge_features));
    }
	
	public static void main(String[] args) {
		ProblemSetup problemSetup = new ProblemSetup(args);
		
		try {
		
			DataSetDescription dataDesc = new DataSetDescription();
			dataDesc.loadFromJsonFile(problemSetup.dataMetadataFile);
			
			FastSharder<Integer, RatingEdge> sharder = AggregateRecommender.createSharder(dataDesc.getRatingsFile(), 
					problemSetup.nShards, 0); 
			IO.convertMatrixMarket(dataDesc.getRatingsFile(), problemSetup.nShards, sharder);
			
	    	GraphChiProgram<Integer, RatingEdge> aggRec = new AggregateRecommender(dataDesc, problemSetup.paramFile);
	    	
	        /* Run GraphChi */
	        GraphChiEngine<Integer, RatingEdge> engine = new GraphChiEngine<Integer, RatingEdge>(dataDesc.getRatingsFile(),
	        	problemSetup.nShards);
	        
	        //TODO: Set edge features properly
	        engine.setEdataConverter(new RatingEdgeConvertor(0) );
	        engine.setEnableDeterministicExecution(false);
	        engine.setVertexDataConverter(null);  // We do not access vertex values.
	        engine.setModifiesInedges(false); // Important optimization
	        engine.setModifiesOutedges(false); // Important optimization
	        engine.run(aggRec, 5);
	
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(2);
		}
	}
	
	
}
		
class RatingEdge {
	float observation;
	int[] featureId;
	float[] featureVal;
	
	public RatingEdge() {
		this.observation = 0;
	}
	
	public RatingEdge(float observation, int[] featureId, float[] featureVal) {
		this.observation = observation;
		this.featureId = featureId;
		this.featureVal = featureVal;
	}
}


class RatingEdgeConvertor implements  BytesToValueConverter<RatingEdge> {
	int numFeatures;
	
    public int sizeOf() {
        return 4 + numFeatures*8;
    }
    
    public RatingEdgeConvertor(int numFeatures) {
    	this.numFeatures = numFeatures;
    }
    
    public RatingEdge getValue(byte[] array) {
    	RatingEdge res = null;
    	
    	ByteBuffer buf = ByteBuffer.wrap(array);
    	float obs = buf.getFloat(0);
    	
    	int[] featureIds = new int[this.numFeatures];
    	float[] featureVals = new float[this.numFeatures];
    	
    	for(int i = 0; i < this.numFeatures; i++ ) {
    		featureIds[i] = buf.getInt(4 + i*8);
    		featureVals[i] = buf.getFloat(4 + i*8 + 4);
    	}
    	
    	res = new RatingEdge(obs, featureIds, featureVals);
    	return res;
    }
    
    public void setValue(byte[] array, RatingEdge val) {
    	ByteBuffer buf = ByteBuffer.allocate(sizeOf());
    	buf.putFloat(0, val.observation);
	  
    	for(int i = 0; i < this.numFeatures; i++ ) {
    		buf.putInt(4 + i*8, val.featureId[i]);
    		buf.putFloat(4 + i*8 + 4, val.featureVal[i]);
    	}
    	byte[] a = buf.array();
    	
    	for(int i = 0; i < a.length; i++) {
    		array[i] = a[i];
    	}
    }
}

class RatingEdgeProcessor implements EdgeProcessor<RatingEdge> {

	@Override
	public RatingEdge receiveEdge(int from, int to, String token) {
		if(token == null) {
			return new RatingEdge();
		} else {
			String[] tokens = token.split("\t");
			float obs = Float.parseFloat(tokens[0]);
			
			if(tokens.length == 1) {
				return new RatingEdge(obs, null, null);
			}
			
			int[] featureIds = new int[tokens.length - 1];
			float[] featureVals = new float[tokens.length - 1];
			
			for(int i = 1; i < tokens.length; i++) {
				featureIds[i] = Integer.parseInt(tokens[i].split(":")[0]);
				featureVals[i] = Float.parseFloat(tokens[i].split(":")[1]);
			}
			
			RatingEdge e = new RatingEdge(obs , featureIds, featureVals);
			
			return e;
		}
	}
	
}
