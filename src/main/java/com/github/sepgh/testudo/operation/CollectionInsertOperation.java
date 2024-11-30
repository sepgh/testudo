package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.SerializationException;

public interface CollectionInsertOperation<T extends Number & Comparable<T>> {
    <V> void insert(V v) throws SerializationException;
    void insert(byte[] bytes);
}
