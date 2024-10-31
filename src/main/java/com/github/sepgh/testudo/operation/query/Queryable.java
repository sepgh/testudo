package com.github.sepgh.testudo.operation.query;

import com.github.sepgh.testudo.exception.InternalOperationException;

import java.util.Iterator;

public interface Queryable<K extends Comparable<K>, V> {
    Iterator<V> getGreaterThan(K k, Order order) throws InternalOperationException;
    Iterator<V> getGreaterThanEqual(K k, Order order) throws InternalOperationException;
    Iterator<V> getLessThan(K k, Order order) throws InternalOperationException;
    Iterator<V> getLessThanEqual(K k, Order order) throws InternalOperationException;
    Iterator<V> getEqual(K k, Order order) throws InternalOperationException;
}
