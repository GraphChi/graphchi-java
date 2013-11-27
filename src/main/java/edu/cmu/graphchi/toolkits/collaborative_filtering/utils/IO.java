package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.logging.Logger;

import org.apache.commons.math.linear.RealVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.AggregateRecommender;
import edu.cmu.graphchi.util.HugeDoubleMatrix;


public class IO {
    
    public static final String HDFS_PREFIX = "hdfs://";
    public static final String LOCAL_FS_PREFIX = "file://";
    public static final String URL_FS_PREFIX = "https://";
    
    public static Configuration getConf() {
        Configuration conf = new Configuration();
        conf.addResource(new Path(System.getenv().get("HADOOP_CONF_DIR") + "/core-site.xml"));
        conf.addResource(new Path(System.getenv().get("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
        return conf;
    }
    
	static final String MATRIX_MARKET_BANNER = "%MatrixMarket matrix array real general";
	static protected Logger logger = ChiLogger.getLogger("IO");
	
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
			} else if (trainingFileLocation.startsWith(URL_FS_PREFIX)) {
			    //Open URL for sharding
                URL ratingsUrl = new URL(trainingFileLocation);
                BufferedInputStream inStream = new BufferedInputStream((ratingsUrl.openStream()));
                sharder.shard(inStream, FastSharder.GraphInputFormat.MATRIXMARKET);
                
			} else {
				
			}
      } else {
          //problemSetup.logger.info("Found shards -- no need to preprocess");
      }
	}
	
	private static void mmOutputBanner(PrintWriter writer, String banner){
		writer.println(banner);
	}
	private static void mmOutputMatrixSize(PrintWriter writer, long nRow, long nCol){
		writer.println(nRow + "\t" + nCol);
	}
	public static void mmOutputMatrix(String filename, int start, int end, HugeDoubleMatrix latentFactors, String comment){
		long nCol = latentFactors.getNumCols();
		assert(start < end);
		try {
			PrintWriter writer = new PrintWriter(filename);
			mmOutputBanner(writer, MATRIX_MARKET_BANNER);
			writer.println("%" + comment);
			mmOutputMatrixSize(writer, end - start, nCol);			
			for(int row = start ; row < end ; row++){
				double rowBlock[] = latentFactors.getRowBlock(row);
				latentFactors.getRow(row,rowBlock);
				for( int col = 0 ; col < nCol ; col++){
					writer.println(rowBlock[col]);
				}
			}
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			logger.warning("FileNotFoundException: "+filename);
		}
	}
	
	public static void mmOutputVector(String filename, int start, int end, RealVector vector, String comment){
		assert(start < end);
		try{
			PrintWriter writer = new PrintWriter(filename);
			mmOutputBanner(writer, MATRIX_MARKET_BANNER);
			writer.println("%" + comment);
			mmOutputMatrixSize(writer, end - start, 1);			
			for(int row = start ; row < end ; row++){
				writer.println(vector.getEntry(row));
			}
			writer.close();
		} catch (FileNotFoundException e){
			logger.warning("FileNotFoundException: "+filename);
		}
	}
	
	
	//For testing purposes
    public static void main(String[] args) {
        try {
            //String hdfsFileLocation = "file:///media/sda5/Capstone/Movielens/ml-100k/working_dir/u.data_tr1.mm";
            //String baseFileLocation = "/media/sda5/Capstone/Movielens/ml-100k/working_dir/u.data_tr1.mm";
            String hdfsFileLocation = "hdfs://localhost:9000/user/hdfs/Movielens/ml-100k/working_dir/u.data_tr1.mm";
            String baseFileLocation = "file:///home/mayank/repos/graphchi-java/tmp/abc";
            
            
            //IO.convertMatrixMarket(baseFileLocation, hdfsFileLocation, 3, sharder);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
