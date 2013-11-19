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
 * An object of this class represents a list of recommenders to be run by
 * in a single AggregateRecommender program. The algorithms run will save
 * on cost of IO and will be run in a pipelined way. Thus, if there are total
 * of 10 recommenders that are present in the pool, but only 4 can be run at a time 
 * then the 4 will be initially run and once a slot becomes empty, other recommenders
 * might start running.
 * @author mayank
 */

public class RecommenderPool {

	private List<RecommenderAlgorithm> allRecommenders;
	//Ids (indices in the allRecommender's list) which are pending.
	private Set<Integer> pendingRecommenders;
	//Ids (indices in the allRecommender's list) which are pending.
	private Set<Integer> activeRecommenders;

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
	
	/**
	 * This method allows adding new recommenders and adds them to the list of
	 * pending recommenders. Hence, the size of recommender pool might vary dynamically.
	 * @param rec: The recommender to be added to the recommender pool.
	 */
	public void addNewRecommender(RecommenderAlgorithm rec) {
		//Add the new recommeder to pending list
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
	
	//return a copy of active recommenders to ensure that activeRecommeders is immutable.
	//Since the list is small, its ok to create a new list everytime?
	public Set<Integer> getActiveRecommenders(){
		return  new HashSet<Integer>(this.activeRecommenders);
	}
	
	//return a copy of pending recommenders to ensure that activeRecommeders is immutable
	//Since the list is small, its ok to create a new list everytime?
	public Set<Integer> getPendingRecommenders(){
		return new HashSet<Integer>(this.pendingRecommenders);
	}
	
	/**
	 * Gets the ith recommender from the list of recommenders in the pool 
	 * @param i: The index of the recommender in this pool
	 * @return : The ith recommender in the pool
	 */
	public RecommenderAlgorithm getRecommender(int i) {
		return this.allRecommenders.get(i);
	}
	
	public int getRecommenderPoolSize() {
		return this.allRecommenders.size();
	}
	
	/**
	 * Set all these recommenders as completed. Note that the list of recommender ids might
	 * be active or pending. This method will mark them as complete.
	 * @param ids: Ids of recommenders to be marked as completed. 
	 */
	public void setRecommedersAsCompleted(List<Integer> ids){
		for(int i : ids) {
			this.pendingRecommenders.remove(new Integer(i));
			boolean wasActive = this.activeRecommenders.remove(new Integer(i));
			if(wasActive) {
				this.currentMemoryUsed -= this.allRecommenders.get(i).getEstimatedMemoryUsage();
			}
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
	 * Creates a json file which represents all the recommender algorithms to be run in this pool.
	 * This is needed to ship this pool of recommenders to some other host in case of Apache YARN.
	 * The new config file created will be used to instantiate the graphchi program on the other host
	 * and run the program.
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
