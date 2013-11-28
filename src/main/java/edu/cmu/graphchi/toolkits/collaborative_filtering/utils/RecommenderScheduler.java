package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;

import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.RecommenderAlgorithm;


/**
 * This class contains the logic to take a bunch of recommenders configured to run and
 * split them into separate "recommender pools". Each pool of recommender runs in a single GraphChi
 * program (one AggregateRecommender drives all these recommenders). Note that aggregate
 * recommender can probably run only some (for example, k) recommenders where k depends
 * on the available memory in JVM and the amount of memory each recommender
 * takes. However, a group might contain more than k recommenders. The other recommenders 
 * are in the pool can be started as and when memory becomes free due to convergence of 
 * each recommender. Aggregate recommender after each iteration checks for convergence
 * of all the recommenders and computes the amount of free memory so that it can choose
 * other recommenders in the pool to run
 * @author mayank
 */

public class RecommenderScheduler {
	private List<RecommenderAlgorithm> allRecommenders;
	List<Container> containers;
	
	public RecommenderScheduler(List<Container> containers, List<RecommenderAlgorithm> recommenders) {
		this.allRecommenders = recommenders;
		this.containers = containers;
	}
	
	public List<RecommenderPool> splitIntoRecPools(DataSetDescription datasetDesc, int numShards) {
		// Current Naive algorithm: Assume all resources are equal and 
	    // greedily divide into pools proportional to number of resources
	    // available in the list of resources. 
	    // The correct solution to this problem is essentially a bin packing problem.
	    
		int currMemConsumed = 0;
		List<RecommenderPool> recPools = new ArrayList<RecommenderPool>();
		RecommenderPool currRecPool = new RecommenderPool(datasetDesc, null, numShards);
		recPools.add(currRecPool);
		
		int count = 0;
		
		for(RecommenderAlgorithm rec : allRecommenders) {
			if (recPools.size() < containers.size()) {
			    //Greedily fill all the resources available upto the maximum (less than total available memory)
			    
			    //Memory requirement of the new recommender
			    int mem = rec.getEstimatedMemoryUsage();
			    
			    //Computing max memory available in this container for recommenders.
			    int currContainerMem = this.containers.get(count).getResource().getMemory();
			    int maxAvailableMem = currRecPool.computeMaxAvailableMemory(currContainerMem);
			    
			    if(currMemConsumed + mem < maxAvailableMem) {
			        currRecPool.addNewRecommender(rec);
			        currMemConsumed += mem;
			    } else 
			        currRecPool = new RecommenderPool(datasetDesc, null, numShards);
				    currRecPool.addNewRecommender(rec);
				    recPools.add(currRecPool);
				    currMemConsumed = mem;
			} else {
			    //All the resources are filled up to their limit. Now for the remaining jobs,
			    //just add to each pool one by one.
			    recPools.get(count%recPools.size()).addNewRecommender(rec);
			}
			count++;
		}
		
		return recPools;
	}
	
	
	//For testing
	public static void main(String[] args) {
	    DataSetDescription dataDesc = new DataSetDescription("/media/sda5/Capstone/Netflix/netflix_mm_desc.json");
	    
	    System.out.println();
	    
	    
	}
	
}
