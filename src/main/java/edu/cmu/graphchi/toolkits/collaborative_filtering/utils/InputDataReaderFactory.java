package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

/**
 * This class creates the suitable input data reader based on the URL in the DataSetDescription object. 
 * @author mayank
 */

public class InputDataReaderFactory {

	public InputDataReaderFactory(DataSetDescription datasetDesc) {
		
	}
	
	public static InputDataReader createInputDataReader(DataSetDescription datasetDesc) {
		if(datasetDesc.getRatingsUrl().startsWith(IO.HDFS_PREFIX)) {
			return new HDFSInputDataReader(datasetDesc);
		} else if (datasetDesc.getRatingsUrl().startsWith(IO.LOCAL_FS_PREFIX)) {
			return new FileInputDataReader(datasetDesc);
		} else {
			return null;
		}
		
	}
	

}
