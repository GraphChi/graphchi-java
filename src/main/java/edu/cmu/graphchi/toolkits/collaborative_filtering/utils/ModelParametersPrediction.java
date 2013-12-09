package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

/**
 * The class is used for predicting a new testing file. The class should include the model object,
 * the error measurement and the location of output prediction file.
 * @author shuhaoyu
 *
 */
public class ModelParametersPrediction {
	private final ModelParameters params;
	private final ErrorMeasurement errorMeasure;
	private final String outputFile;
	public ModelParametersPrediction(ModelParameters params,
			ErrorMeasurement errorMeasure, String outputFile) {
		this.params = params;
		this.errorMeasure = errorMeasure;
		this.outputFile = outputFile;
	}
	public ModelParameters getParams() {
		return params;
	}
	public String getOutputFile() {
		return outputFile;
	}
	public void addErrorInstance(double rating, double prediction){
		errorMeasure.incrementErrorInstance(rating, prediction);
	}
	public double getFinalError(){
		return errorMeasure.getFinalErrorRate();
	}
	public String getErrorTypeString(){
		return errorMeasure.getErrorTypeString();
	}
}
