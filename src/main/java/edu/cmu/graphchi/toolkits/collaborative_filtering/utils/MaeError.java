package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

public class MaeError implements ErrorMeasurement {
	private int numInstance;
	private double errorSum;
	public MaeError() {	
		numInstance = 0;
		errorSum = 0.0;
	}
	@Override
	public double incrementErrorInstance(double rating, double prediction) {
		numInstance++;
		errorSum += Math.abs(rating - prediction);
		return 0;
	}
	@Override
	public double getFinalErrorRate() {
		if(numInstance == 0){
			throw new ArithmeticException("Division by zero: No instance predicted yet");
		}
		return (errorSum / numInstance);
	}
	@Override
	public String getErrorTypeString() {
		return "MAE";
	}

}
