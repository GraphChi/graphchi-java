package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import scala.Array;

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
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.InputDataReader;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.InputDataReaderFactory;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.RecommenderPool;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.RecommenderScheduler;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.VertexDataCache;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;

public class AggregateRecommender implements
		GraphChiProgram<Integer, RatingEdge> {
	
	private DataSetDescription datasetDesc;
	
	public RecommenderPool recPool;
	public Set<Integer> activeRecommenderIds;
	
	//Contains data about user and item features. Currently this is held in memory.
	VertexDataCache vertexDataCache = null;
	
	public AggregateRecommender(DataSetDescription datasetDesc, RecommenderPool pool) {
		this.datasetDesc = datasetDesc;
		this.recPool = pool;
	}

	@Override
	public void update(ChiVertex<Integer, RatingEdge> vertex,
			GraphChiContext context) {
		for(int i : this.activeRecommenderIds) {
			RecommenderAlgorithm rec = this.recPool.getRecommender(i);
			rec.update(vertex, context);
		}
	}

	@Override
	public void beginIteration(GraphChiContext ctx) {
		this.activeRecommenderIds = recPool.getActiveRecommenders();
		for(int i : this.activeRecommenderIds) {
			RecommenderAlgorithm rec = this.recPool.getRecommender(i);
			rec.beginIteration(ctx);
		}
	}

	@Override
	public void endIteration(GraphChiContext ctx) {
		List<Integer> completedRec = new ArrayList<Integer>();
		for(int i : this.activeRecommenderIds) {
			RecommenderAlgorithm rec = this.recPool.getRecommender(i);
			rec.endIteration(ctx);
			
			if(rec.hasConverged(ctx)) {
				//Mark this recommender
				completedRec.add(i);
				//Compute the validation error
				try {
					double validationRMSE = 0;
					if(this.datasetDesc.getValidationUrl() != null) {
			        	DataSetDescription valDataDesc = new DataSetDescription();
			        	valDataDesc.setRatingsUrl(this.datasetDesc.getValidationUrl());
			        	InputDataReader reader = InputDataReaderFactory.createInputDataReader(valDataDesc);
			        	reader.initRatingData();
			        	
			        	long count = 0;
			        	while(reader.nextRatingData()) {
			        		int userId = reader.getCurrRatingFrom();
			        		int itemId = reader.getCurrRatingTo();
			        		
			        		SparseVector userFeatures = null;
			        		if(this.vertexDataCache != null) {
			        			userFeatures = this.vertexDataCache.getFeatures(userId);
			        		}
			        		
			        		SparseVector itemFeatures = null;
			        		if(this.vertexDataCache != null) {
			        			itemFeatures = this.vertexDataCache.getFeatures(itemId); 
			        		}
			        		//TODO: Figure out how to support edge values.
			        		SparseVector edgeFeatures = null;
			        		
			        		double estVal = rec.getParams().predict(userId, itemId, 
			        				userFeatures, itemFeatures, edgeFeatures, datasetDesc);
			        		
			        		validationRMSE += (reader.getCurrRating() - estVal)*(reader.getCurrRating() - estVal);
			        		count++;
			        	}
			        	validationRMSE = Math.sqrt(validationRMSE/(1.0*count));
			        }
					System.out.println("Finished Recommender " + rec.getParams().getId() + " Validation RMSE: " + validationRMSE);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		//Mark all recommenders that has completed
		this.recPool.setRecommedersAsCompleted(completedRec);
		
		if(this.recPool.getPendingRecommenders().size() == 0 && this.recPool.getActiveRecommenders().size() == 0) {
			//All recommenders have successfully finished. Break out of the loop
		    ctx.setFinishComputation(); 
		}
	}

	@Override
	public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
		for(int i : this.activeRecommenderIds) {
			RecommenderAlgorithm rec = this.recPool.getRecommender(i);
			rec.beginInterval(ctx, interval);
		}
	}

	@Override
	public void endInterval(GraphChiContext ctx, VertexInterval interval) {
		for(int i : this.activeRecommenderIds) {
			RecommenderAlgorithm rec = this.recPool.getRecommender(i);
			rec.endInterval(ctx, interval);
		}
		
	}

	@Override
	public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
		for(int i : this.activeRecommenderIds) {
			RecommenderAlgorithm rec = this.recPool.getRecommender(i);
			rec.beginSubInterval(ctx, interval);
		}
	}

	@Override
	public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
		for(int i : this.activeRecommenderIds) {
			RecommenderAlgorithm rec = this.recPool.getRecommender(i);
			rec.endSubInterval(ctx, interval);
		}		
	}

	public static FastSharder createSharder(String graphName, int numShards, int num_edge_features) throws IOException {
        return new FastSharder<Integer, RatingEdge>(graphName, numShards, null, 
        		new RatingEdgeProcessor(), 
        	new IntConverter(), new RatingEdgeConvertor(num_edge_features));
    }
	
	public static void main(String[] args) {
		ProblemSetup problemSetup = new ProblemSetup(args);
		
		try {
		
			DataSetDescription dataDesc = new DataSetDescription();
			dataDesc.loadFromJsonFile(problemSetup.dataMetadataFile);
			
			FastSharder<Integer, RatingEdge> sharder = AggregateRecommender.createSharder(problemSetup.scratchDir, 
					problemSetup.nShards, 0); 
			IO.convertMatrixMarket(problemSetup.scratchDir, dataDesc.getRatingsUrl(), problemSetup.nShards, sharder);
			
			VertexDataCache vertexDataCache = VertexDataCache.createVertexDataCache(dataDesc);
			List<RecommenderAlgorithm> recommenders = RecommenderFactory.buildRecommenders(dataDesc, 
					problemSetup.paramFile, vertexDataCache);

			int heapMemory = (int)Runtime.getRuntime().maxMemory() / (1024*1024);
			//TODO: Estimate the memory used by shards and other GraphChiEngine related stuff.
			//Should mainly include memory used by some internal data structures, shards and vertexDataCache.
			int engineMemoryRequirement = 0;
			//The remaining memory available to recommenders for storing model parameters 
			int maxAvailableMemory = heapMemory - engineMemoryRequirement;
			RecommenderScheduler sched = new RecommenderScheduler(1, maxAvailableMemory, recommenders);
			//RecommenderScheduler sched = new RecommenderScheduler(1, 70, recommenders);
			
			List<RecommenderPool> recPool = sched.splitIntoRecPools();
			
			recPool.get(0).resetPool();
			AggregateRecommender aggRec = new AggregateRecommender(dataDesc, recPool.get(0));
			aggRec.vertexDataCache = vertexDataCache;
	    	
	        /* Run GraphChi */
	        GraphChiEngine<Integer, RatingEdge> engine = new GraphChiEngine<Integer, RatingEdge>(problemSetup.scratchDir,
	        	problemSetup.nShards);
	        
	        //TODO: Set edge features properly
	        engine.setEdataConverter(new RatingEdgeConvertor(0) );
	        engine.setEnableDeterministicExecution(false);
	        engine.setVertexDataConverter(null);  // We do not access vertex values.
	        engine.setModifiesInedges(false); // Important optimization
	        engine.setModifiesOutedges(false); // Important optimization
	        engine.run(aggRec, 20);
	
		    //TODO: Persist models - Serialization has not yet been implemented
	        for(int i = 0; i < aggRec.recPool.getRecommenderPoolSize(); i++) {
	        	RecommenderAlgorithm rec = aggRec.recPool.getRecommender(i);
	        	rec.getParams().serialize(problemSetup.outputLoc);
	        }
	        
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(2);
		}
	}
	
	
}
