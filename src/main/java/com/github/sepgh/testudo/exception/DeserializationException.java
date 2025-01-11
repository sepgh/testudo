package com.github.sepgh.testudo.exception;

public class DeserializationException extends BaseSerializationException {
    private static final String MESSAGE_PREPEND = "Deserialization Error: ";

    public DeserializationException() {
        super(MESSAGE_PREPEND);
    }

    public DeserializationException(String message) {
        super(MESSAGE_PREPEND + message);
    }

    public DeserializationException(String message, Throwable cause) {
        super(MESSAGE_PREPEND + message, cause);
    }

    public DeserializationException(Throwable cause) {
        super(cause);
    }

}
