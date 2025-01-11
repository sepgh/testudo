package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.SerializationException;

public interface CollectionInsertOperation<T extends Number & Comparable<T>> {
    <V> V execute(V v) throws SerializationException, InternalOperationException, DeserializationException;
    byte[] execute(byte[] bytes) throws InternalOperationException, DeserializationException, SerializationException;
}
