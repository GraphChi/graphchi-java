package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

public class ModelParametersPrediction {
	private final ModelParameters params;
	private final String outputFile;
	public ModelParametersPrediction(ModelParameters params, String outputFile) {
		this.params = params;
		this.outputFile = outputFile;
	}
	public ModelParameters getParams() {
		return params;
	}
	public String getOutputFile() {
		return outputFile;
	}
}
