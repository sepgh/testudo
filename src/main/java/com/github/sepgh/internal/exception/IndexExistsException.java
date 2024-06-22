package com.github.sepgh.internal.exception;

public class IndexExistsException extends Exception {
    public IndexExistsException() {
        super("Index already exists!");
    }
}
