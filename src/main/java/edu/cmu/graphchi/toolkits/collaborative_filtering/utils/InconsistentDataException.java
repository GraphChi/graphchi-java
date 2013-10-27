package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

public class InconsistentDataException extends Exception {

	public InconsistentDataException() {
	}

	public InconsistentDataException(String message) {
		super(message);
	}

	public InconsistentDataException(Throwable cause) {
		super(cause);
	}

	public InconsistentDataException(String message, Throwable cause) {
		super(message, cause);
	}

	public InconsistentDataException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
