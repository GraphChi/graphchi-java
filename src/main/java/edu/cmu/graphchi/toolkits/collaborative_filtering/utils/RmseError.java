package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

public class RmseError implements ErrorMeasurement {
	private int numInstance;
	private double errorSum;
	public RmseError() {	
		numInstance = 0;
		errorSum = 0.0;
	}

	@Override
	public double incrementErrorInstance(double rating, double prediction) {
		numInstance ++;
		errorSum += (rating - prediction) * (rating - prediction);
		return 0;
	}

	@Override
	public double getFinalErrorRate() {	
		if(numInstance == 0){
			throw new ArithmeticException("Division by zero: No instance predicted yet");
		}
		return Math.sqrt(errorSum / numInstance);
	}

	@Override
	public String getErrorTypeString() {
		return "RMSE";
	}

}
