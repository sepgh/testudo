package com.github.sepgh.testudo.exception;

public class BaseSerializationException extends Exception {
    public BaseSerializationException() {
    }

    public BaseSerializationException(String message) {
        super(message);
    }

    public BaseSerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public BaseSerializationException(Throwable cause) {
        super(cause);
    }

}
