package com.github.sepgh.testudo.exception;

public class VerificationException {

    public static class InvalidDBObjectWrapper extends Exception {
        public InvalidDBObjectWrapper(String message) {
            super(message);
        }
    }

}
