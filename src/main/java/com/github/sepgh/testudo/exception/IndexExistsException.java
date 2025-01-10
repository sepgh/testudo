package com.github.sepgh.testudo.exception;

public class IndexExistsException extends InternalOperationException {
    public IndexExistsException(String key) {
        super("Index already exists: " + key);
    }
}
