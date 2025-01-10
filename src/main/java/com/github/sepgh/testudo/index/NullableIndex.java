package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.operation.query.Order;

import java.util.Iterator;

public interface NullableIndex<V> {
    default boolean isNull(V value) {
        throw new UnsupportedOperationException();
    }

    default void addNull(V value) throws InternalOperationException {
        throw new UnsupportedOperationException();
    }

    default void removeNull(V value) throws InternalOperationException {
        throw new UnsupportedOperationException();
    }

    default Iterator<V> getNullIndexes(Order order) {
        throw new UnsupportedOperationException();
    }

}
