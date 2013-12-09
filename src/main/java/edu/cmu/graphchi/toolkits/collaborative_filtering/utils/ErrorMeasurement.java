package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

public interface ErrorMeasurement {
	public double incrementErrorInstance(double rating, double prediction);
	public double getFinalErrorRate();
	public String getErrorTypeString();
}
