package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.AggregateRecommender;

public class IO {
	public static final String HDFS_PREFIX = "hdfs://";
	public static final String LOCAL_FS_PREFIX = "file://";
	
	public static Configuration getConf() {
		Configuration conf = new Configuration();
		conf.addResource(new Path(System.getenv().get("HADOOP_CONF_DIR") + "/core-site.xml"));
		conf.addResource(new Path(System.getenv().get("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
		return conf;
	}
	
	public static void convertMatrixMarket(String trainingFile, int nshards, FastSharder sharder) 
			throws IOException{
		  /* Run sharding (preprocessing) if the files do not exist yet */
      if (!new File(ChiFilenames.getFilenameIntervals(trainingFile, nshards)).exists() ||
              !new File(trainingFile + ".matrixinfo").exists()) {
          sharder.shard(new FileInputStream(new File(trainingFile)), FastSharder.GraphInputFormat.MATRIXMARKET);
      } else {
          //problemSetup.logger.info("Found shards -- no need to preprocess");
      }
	}
	
	public static void convertMatrixMarket(String baseFileName, String trainingFileLocation, 
		int nshards, FastSharder sharder) throws IOException{
		/* Run sharding (preprocessing) if the files do not exist yet */
		if (!new File(ChiFilenames.getFilenameIntervals(baseFileName, nshards)).exists() ||
              !new File(baseFileName + ".matrixinfo").exists()) {
			
			if(trainingFileLocation.startsWith(HDFS_PREFIX)) {
				//String path = trainingFileLocation.substring(HDFS_PREFIX.length());
				Path hdfsPath = new Path(trainingFileLocation);
				
				Configuration conf = getConf();
				
				FileSystem fs = FileSystem.get(conf);
				
				System.out.println(hdfsPath);
				if(!fs.exists(hdfsPath)) {
					throw new IOException("File not found in HDFS");
				}
				
				sharder.shard(fs.open(hdfsPath), FastSharder.GraphInputFormat.MATRIXMARKET);
			} else if (trainingFileLocation.startsWith(LOCAL_FS_PREFIX)) {
				String path = trainingFileLocation.substring(LOCAL_FS_PREFIX.length());
				sharder.shard(new FileInputStream(new File(path)), FastSharder.GraphInputFormat.MATRIXMARKET);
			} else {
				
			}
      } else {
          //problemSetup.logger.info("Found shards -- no need to preprocess");
      }
	}
	

	//For testing purposes
	public static void main(String[] args) {
		try {
			//String hdfsFileLocation = "file:///media/sda5/Capstone/Movielens/ml-100k/working_dir/u.data_tr1.mm";
			//String baseFileLocation = "/media/sda5/Capstone/Movielens/ml-100k/working_dir/u.data_tr1.mm";
			String hdfsFileLocation = "hdfs://localhost:9000/user/hdfs/Movielens/ml-100k/working_dir/u.data_tr1.mm";
			String baseFileLocation = "/home/mayank/repos/graphchi-java/tmp/abc";
			
			FastSharder sharder = AggregateRecommender.createSharder(baseFileLocation, 3, 0); 
			IO.convertMatrixMarket(baseFileLocation, hdfsFileLocation, 3, sharder);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    
}
