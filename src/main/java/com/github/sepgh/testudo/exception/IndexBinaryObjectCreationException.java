package com.github.sepgh.testudo.exception;

public class IndexBinaryObjectCreationException extends InternalOperationException {
    public static final String PREFIX = "Failed to create IndexBinaryObject: ";
    public IndexBinaryObjectCreationException(String message) {
        super(PREFIX + message);
    }
}
