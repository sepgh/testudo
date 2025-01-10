package com.github.sepgh.testudo.index.data;

import com.github.sepgh.testudo.exception.DeserializationException;

public interface IndexBinaryObject<E> {
    E asObject() throws DeserializationException;
    int size();
    byte[] getBytes();
    default E getFirst() {
        throw new UnsupportedOperationException();
    }
    default E getNext(E current) {
        throw new UnsupportedOperationException();
    }
    default E getNext() throws DeserializationException {
        throw new UnsupportedOperationException();
    }
    default boolean supportIncrements() {
        return false;
    }
}
