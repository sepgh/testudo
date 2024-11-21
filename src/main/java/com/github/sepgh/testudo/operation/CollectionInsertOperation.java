package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.SerializationException;

public interface CollectionInsertOperation {
    <V> void insert(V v) throws SerializationException;
    <T extends Number & Comparable<T>> void insert(byte[] bytes);
}
