package com.github.sepgh.testudo.exception;

public class IndexMissingException extends Exception {
    public IndexMissingException() {
        super("Index is not found!");
    }
}
