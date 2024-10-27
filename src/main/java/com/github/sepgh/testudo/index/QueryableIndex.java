package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.InternalOperationException;

import java.util.Iterator;
import java.util.Optional;

public interface QueryableIndex<K extends Comparable<K>, V> {
    Iterator<V> getGreaterThan(K k) throws InternalOperationException;
    Iterator<V> getGreaterThanEqual(K k) throws InternalOperationException;
    Iterator<V> getLessThan(K k) throws InternalOperationException;
    Iterator<V> getLessThanEqual(K k) throws InternalOperationException;
    Optional<V> getEqual(K k) throws InternalOperationException;
}
