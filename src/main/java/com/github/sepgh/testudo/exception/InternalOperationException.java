package com.github.sepgh.testudo.exception;

public class InternalOperationException extends Exception {
    private static final String MESSAGE_PREPEND = "Testudo core error";

    public InternalOperationException() {
        super(MESSAGE_PREPEND);
    }

    public InternalOperationException(String message) {
        super(MESSAGE_PREPEND + message);
    }

    public InternalOperationException(String message, Throwable cause) {
        super(MESSAGE_PREPEND + message, cause);
    }

    public InternalOperationException(Throwable cause) {
        super(cause);
    }

    public InternalOperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(MESSAGE_PREPEND + message, cause, enableSuppression, writableStackTrace);
    }
}
