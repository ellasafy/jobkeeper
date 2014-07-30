package com.cstor.exception;

public class JkException extends RuntimeException {

	private static final long serialVersionUID = -6954629315793836563L;

	public JkException() {
		super();
	}

	public JkException(String message, Throwable cause) {
		super(message, cause);
	}

	public JkException(String message) {
		super(message);
	}

	public JkException(Throwable cause) {
		super(cause);
	}
}
