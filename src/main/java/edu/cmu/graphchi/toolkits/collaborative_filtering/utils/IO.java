package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Logger;

import org.apache.commons.math.linear.RealVector;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.util.HugeDoubleMatrix;

public class IO {
	static final String MATRIX_MARKET_BANNER = "%MatrixMarket matrix array real general";
	static protected Logger logger = ChiLogger.getLogger("IO");
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
    
}
