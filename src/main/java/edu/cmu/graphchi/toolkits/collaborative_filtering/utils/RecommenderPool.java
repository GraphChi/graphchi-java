package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;

import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.AggregateRecommender;
import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.RatingEdge;
import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.RecommenderAlgorithm;
import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.RecommenderFactory;


/**
 * 
 * @author mayank
 */

public class RecommenderPool {

	public List<RecommenderAlgorithm> allRecommenders;
	//Ids (indices in the allRecommender's list) which are pending.
	public Set<Integer> pendingRecommenders;
	//Ids (indices in the allRecommender's list) which are pending.
	public Set<Integer> activeRecommenders;

	//Maximum available memory for the place where this pool is going to run.
	private int maxAvailableMemory;
	
	//Current memory used by active recommenders.
	private int currentMemoryUsed;
	
	public RecommenderPool(int maxAvailableMemory) {
		this.maxAvailableMemory = maxAvailableMemory;
		this.currentMemoryUsed = 0;
		this.allRecommenders = new ArrayList<RecommenderAlgorithm>();
		this.pendingRecommenders = new HashSet<Integer>();
		this.activeRecommenders = new HashSet<Integer>();
	}
	
	public void addNewRecommender(RecommenderAlgorithm rec) {
		this.pendingRecommenders.add(this.allRecommenders.size());
		this.allRecommenders.add(rec);
	}
	
	// Set all recommenders to pending and assign some recommenders (based on maxMemory available) as
	// active.
	public void resetPool() {
		for(int i = 0; i < this.allRecommenders.size(); i++) {
			this.pendingRecommenders.add(i);
		}
		this.currentMemoryUsed = 0;
		
		//Naive greedy approach to choose initial set of recommenders.
		for(int i = 0; i < this.allRecommenders.size(); i++) {
			RecommenderAlgorithm rec = this.allRecommenders.get(i);
			if(rec.getEstimatedMemoryUsage() + this.currentMemoryUsed < this.maxAvailableMemory) {
				this.activeRecommenders.add(i);
				this.pendingRecommenders.remove(new Integer(i));
				this.currentMemoryUsed += rec.getEstimatedMemoryUsage();
			}
		}
	}
	
	public Set<Integer> getActiveRecommenders(){
		return this.activeRecommenders;
	}
	
	public RecommenderAlgorithm getRecommender(int i) {
		return this.allRecommenders.get(i);
	}
	
	public void setRecommedersAsCompleted(List<Integer> ids) {
		for(int i : ids) {
			this.activeRecommenders.remove(new Integer(i));
			this.currentMemoryUsed -= this.allRecommenders.get(i).getEstimatedMemoryUsage();
		}
		
		List<Integer> newRec = new ArrayList<Integer>();
		for(int i : this.pendingRecommenders) {
			RecommenderAlgorithm rec = this.allRecommenders.get(i);
			if(currentMemoryUsed + rec.getEstimatedMemoryUsage() < this.maxAvailableMemory) {
				newRec.add(new Integer(i));
				this.activeRecommenders.add(i);
				this.currentMemoryUsed += rec.getEstimatedMemoryUsage();
			}
		}
		this.pendingRecommenders.removeAll(newRec);
		
	}
	
	/**
	 * Creates a json file which represents all the recommender algorithms to be run in this pool
	 * @return
	 */
	public void createParamJsonFile(String fileName) throws Exception {
		List<Map<String, String>> allParams = new ArrayList<Map<String,String>>();
		for(RecommenderAlgorithm rec : this.allRecommenders) {
			allParams.add(rec.getParams().getParamsMap());
		}
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(new File(fileName), allParams);
	}
	
	//For testing purpose only
	public static void main(String[] args) {
		ProblemSetup problemSetup = new ProblemSetup(args);
		
		try {
		
			DataSetDescription dataDesc = new DataSetDescription();
			dataDesc.loadFromJsonFile(problemSetup.dataMetadataFile);
			
			//TODO: Do something else for vertex data cache.
			List<RecommenderAlgorithm> recommenders = RecommenderFactory.buildRecommenders(dataDesc, 
					problemSetup.paramFile, null);
			
			RecommenderScheduler sched = new RecommenderScheduler(1, 70, recommenders);
			List<RecommenderPool> pools = sched.splitIntoRecPools();
			
			pools.get(0).createParamJsonFile("abc.json");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
