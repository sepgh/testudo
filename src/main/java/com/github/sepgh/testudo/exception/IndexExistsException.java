package com.github.sepgh.testudo.exception;

public class IndexExistsException extends Exception {
    public IndexExistsException() {
        super("Index already exists!");
    }
}
