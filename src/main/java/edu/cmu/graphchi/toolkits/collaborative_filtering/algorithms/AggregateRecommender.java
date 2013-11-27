package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.Resource;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.InputDataReader;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.InputDataReaderFactory;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.RatingEdge;
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

	public static void main(String[] args) {
		ProblemSetup problemSetup = new ProblemSetup(args);
		
		try {
		
			DataSetDescription dataDesc = new DataSetDescription(problemSetup.dataMetadataFile);
			
			//Load the vertex data cache
			VertexDataCache vertexDataCache = VertexDataCache.createVertexDataCache(dataDesc);
			
			//Create recommenders.
			List<RecommenderAlgorithm> recommenders = RecommenderFactory.buildRecommenders(dataDesc, 
					problemSetup.paramFile, vertexDataCache);

			for(RecommenderAlgorithm rec : recommenders) {
			    System.out.println("Estimated Mem Usage " + rec.getParams().getId() + " - " + rec.getEstimatedMemoryUsage());
			}
			
			//Create a Pool of Recommenders
			RecommenderPool pool = new RecommenderPool(dataDesc, recommenders, problemSetup.nShards);
			
            AggregateRecommender aggRec = new AggregateRecommender(dataDesc, pool);
            aggRec.vertexDataCache = vertexDataCache;
            
            pool.resetPool();
            
            //Preprocess and load datasets.
            FastSharder<Integer, RatingEdge> sharder = pool.createSharder(problemSetup.scratchDir);
            IO.convertMatrixMarket(problemSetup.scratchDir, dataDesc.getRatingsUrl(), pool.getNumShards(), sharder);
            
            /* Run GraphChi */
            GraphChiEngine<Integer, RatingEdge> engine = new GraphChiEngine<Integer, RatingEdge>(problemSetup.scratchDir,
                    pool.getNumShards());
            
            engine.setEdataConverter(pool.getEdgeDataConvertor());
            engine.setEnableDeterministicExecution(false);
            engine.setVertexDataConverter(null);  // We do not access vertex values.
            engine.setModifiesInedges(pool.isMutable()); // Important optimization
            engine.setModifiesOutedges(pool.isMutable()); // Important optimization
            engine.setMemoryBudgetMb(pool.getMemoryBudget());
            
            // Run for a lot of iterations. If all recommenders in the pool have converged, then they
            // the engin will stop.
            engine.run(aggRec, 1000);
    
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
