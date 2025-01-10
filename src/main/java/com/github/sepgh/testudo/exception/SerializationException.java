package com.github.sepgh.testudo.exception;

public class SerializationException extends Exception {
    private static final String MESSAGE_PREPEND = "Serialization Error";

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

    public SerializationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(MESSAGE_PREPEND + message, cause, enableSuppression, writableStackTrace);
    }
}
