package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.IOException;
import java.util.List;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.FileInputDataReader;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.VertexDataCache;

public class AggregateRecommender implements
		GraphChiProgram<Integer, RatingEdge> {
	
	private DataSetDescription datasetDesc;
	private String modelDescJsonFile;
	
	List<GraphChiProgram> recommenders;
	
	//Contains data about user and item features. Currently this is held in memory.
	VertexDataCache vertexDataCache = null;
	
	public AggregateRecommender(DataSetDescription datasetDesc, String modelDescJsonFile) {
		this.datasetDesc = datasetDesc;
		this.modelDescJsonFile = modelDescJsonFile;
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
		if(ctx.getIteration() == 0) {
			//Initialize the vertex data cache 
			if(this.vertexDataCache == null) {
				int numFeatures = this.datasetDesc.getNumItemFeatures() + 
						this.datasetDesc.getNumUserFeatures() + this.datasetDesc.getNumRatingFeatures();
				int numVertices = this.datasetDesc.getNumUsers() + this.datasetDesc.getNumItems() + 1;
				this.vertexDataCache = new VertexDataCache(numVertices, numFeatures);
				try {
					this.vertexDataCache.loadVertexDataCache(new FileInputDataReader(this.datasetDesc));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			this.recommenders = RecommenderFactory.buildRecommenders(datasetDesc, modelDescJsonFile, vertexDataCache);
		}
		
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
			
			FastSharder<Integer, RatingEdge> sharder = AggregateRecommender.createSharder(dataDesc.getRatingsUrl(), 
					problemSetup.nShards, 0); 
			IO.convertMatrixMarket(dataDesc.getRatingsUrl(), problemSetup.nShards, sharder);
			
	    	GraphChiProgram<Integer, RatingEdge> aggRec = new AggregateRecommender(dataDesc, problemSetup.paramFile);
	    	
	        /* Run GraphChi */
	        GraphChiEngine<Integer, RatingEdge> engine = new GraphChiEngine<Integer, RatingEdge>(dataDesc.getRatingsUrl(),
	        	problemSetup.nShards);
	        
	        //TODO: Set edge features properly
	        engine.setEdataConverter(new RatingEdgeConvertor(0) );
	        engine.setEnableDeterministicExecution(false);
	        engine.setVertexDataConverter(null);  // We do not access vertex values.
	        engine.setModifiesInedges(false); // Important optimization
	        engine.setModifiesOutedges(false); // Important optimization
	        engine.run(aggRec, 5);
	
	        //TODO: Test / Validate models created.
	        
	        //TODO: Persist models
	        
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(2);
		}
	}
	
	
}
