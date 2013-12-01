package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
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
	
	//For testing
	public static void main(String[] args) {
	    DataSetDescription dataDesc = new DataSetDescription("/media/sda5/Capstone/Netflix/netflix_mm_desc.json");
	    
	    System.out.println();
	}

    public static List<RecommenderPool> splitRecommenderPool(
            List<Resource> availableResourcesInNodes,
            List<RecommenderAlgorithm> recommenders,
            DataSetDescription datasetDesc, int nShards) {
        // Current Naive algorithm: Assume all resources are equal and 
        // greedily divide into pools proportional to number of resources
        // available in the list of resources. 
        // The correct solution to this problem is essentially a bin packing problem.
        
        List<RecommenderPool> recPools = new ArrayList<RecommenderPool>();
        List<RecommenderAlgorithm> pendingRec = new ArrayList<RecommenderAlgorithm>();
        for(RecommenderAlgorithm rec : recommenders)
            pendingRec.add(rec);
        
        for(Resource r : availableResourcesInNodes) {
            if (pendingRec.size() < 1) {
                break;
            }
            
            int currMemConsumed = 0;
            RecommenderPool recPool = new RecommenderPool(datasetDesc, null, nShards, r.getMemory());
            int maxAvailableMem = recPool.getMaxAvailableMemory();
            //Greedily fill the recommender pool according to available resources
            for(RecommenderAlgorithm rec : pendingRec){
                if(currMemConsumed + rec.getEstimatedMemoryUsage() < maxAvailableMem) {
                    recPool.addNewRecommender(rec);
                    currMemConsumed += rec.getEstimatedMemoryUsage();
                }
            }
            
            //TODO: There is not equals method implemented in RecommenderAlgorithms. But the reference should 
            //match for RecommenderAlgorithm in recPool and pendingRec and hence this should work ok.
            if(recPool.getAllRecommenders().size() > 0) {
                pendingRec.removeAll(recPool.getAllRecommenders());
                recPools.add(recPool);
            }
            
        }
        
        return recPools;
    }
	
}
