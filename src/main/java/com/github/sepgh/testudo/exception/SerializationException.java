package com.github.sepgh.testudo.exception;

public class SerializationException extends BaseSerializationException {
    private static final String MESSAGE_PREPEND = "Serialization Error: ";

    public SerializationException() {
        super(MESSAGE_PREPEND);
    }

    public SerializationException(String message) {
        super(MESSAGE_PREPEND + message);
    }

    public SerializationException(String message, Throwable cause) {
        super(MESSAGE_PREPEND + message, cause);
    }

    public SerializationException(Throwable cause) {
        super(cause);
    }

}
