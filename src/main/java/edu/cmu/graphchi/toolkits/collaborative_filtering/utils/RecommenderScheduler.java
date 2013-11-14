package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.util.ArrayList;
import java.util.List;

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

/*TODO: Note that currently this scheduler is naive in the sense that it does not take into
 * account a heterogenous set of resources (nodes / GraphChi instances) available with different
 * amounts of memory. In heterogenous case, the splits created might be much more interesting.
 * TODO: Currently, the scheduler has been statically provided number of splits of data to create
 */
public class RecommenderScheduler {
	//Number of splits for recommenders.
	private int numSplits;
	//maximum amount of memory available in JVM. In the case when heterogenous resources
	//are available in the cluster, this might be list of resources.
	private int maxAvailableMemory;
	
	private List<RecommenderAlgorithm> allRecommenders;
	
	public RecommenderScheduler(int numSplits, int maxMemory, List<RecommenderAlgorithm> recommenders) {
		this.numSplits = numSplits;
		this.maxAvailableMemory = maxMemory;
		this.allRecommenders = recommenders;
	}
	
	public List<RecommenderPool> splitIntoRecPools() {
		//Current Naive algorithm, divide into pools so that each pool has 
		//equal amount of memory usage.
		int totalMemory = 0;
		int minMemory = Integer.MAX_VALUE;
		int maxMemory = Integer.MIN_VALUE;
		for(RecommenderAlgorithm rec : allRecommenders) {
			int mem = rec.getEstimatedMemoryUsage();
			
			if(mem < minMemory) {
				minMemory = mem;
			}
			if(mem > maxMemory) {
				maxMemory = mem;
			}
			totalMemory += mem;
		}
		
		int currMemConsumed = 0;
		List<RecommenderPool> recPools = new ArrayList<RecommenderPool>();
		RecommenderPool currRecPool = new RecommenderPool(maxAvailableMemory);
		recPools.add(currRecPool);
		for(RecommenderAlgorithm rec : allRecommenders) {
			int mem = rec.getEstimatedMemoryUsage();
			if(currMemConsumed < totalMemory/numSplits - minMemory || recPools.size() == this.numSplits) {
				currRecPool.addNewRecommender(rec);
				currMemConsumed += mem;
			} else {
				currRecPool = new RecommenderPool(maxAvailableMemory);
				currRecPool.addNewRecommender(rec);
				recPools.add(currRecPool);
				currMemConsumed = mem;
			}
		}
		
		return recPools;
	}
		
}
