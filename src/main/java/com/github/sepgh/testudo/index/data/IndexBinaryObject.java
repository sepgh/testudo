package com.github.sepgh.testudo.index.data;

public interface IndexBinaryObject<E> {
    E asObject();
    int size();
    byte[] getBytes();

    class InvalidIndexBinaryObject extends Exception {
        public InvalidIndexBinaryObject(Object object, Class<? extends IndexBinaryObject<?>> innerObjClass) {
            super(("%s is not a valid value for IndexBinaryObject of type " + innerObjClass).formatted(object.toString()));
        }
    }
}
