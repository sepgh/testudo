package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.SerializationException;

public interface CollectionInsertOperation<T extends Number & Comparable<T>> {
    <V> void execute(V v) throws SerializationException;
    void execute(byte[] bytes);
}
