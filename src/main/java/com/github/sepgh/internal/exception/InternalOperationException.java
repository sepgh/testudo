package com.github.sepgh.internal.exception;

public class InternalOperationException extends Exception {
    public InternalOperationException(Exception exception) {
        super("Operation failed", exception);
    }

    public InternalOperationException(String message, Exception exception) {
        super(message, exception);
    }
}
