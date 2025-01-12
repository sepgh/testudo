package com.github.sepgh.testudo.exception;

public class InternalOperationException extends Exception {
    private static final String PREFIX = "Testudo core error: ";

    public InternalOperationException() {
        super(PREFIX);
    }

    public InternalOperationException(String message) {
        super(PREFIX + message);
    }

    public InternalOperationException(String message, Throwable cause) {
        super(PREFIX + message, cause);
    }

    public InternalOperationException(Throwable cause) {
        super(cause);
    }

    public InternalOperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(PREFIX + message, cause, enableSuppression, writableStackTrace);
    }
}
