package com.github.sepgh.testudo.index.data;

public interface IndexBinaryObject<E> {
    E asObject();
    int size();
    byte[] getBytes();
    default E getFirst() {
        throw new UnsupportedOperationException();
    }
    default E getNext(E current) {
        throw new UnsupportedOperationException();
    }
    default E getNext() {
        throw new UnsupportedOperationException();
    }
    default boolean supportIncrements() {
        return false;
    }
}
