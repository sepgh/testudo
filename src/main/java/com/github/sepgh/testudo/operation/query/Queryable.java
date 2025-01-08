package com.github.sepgh.testudo.operation.query;

import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.utils.IteratorUtils;

import java.util.Iterator;

public interface Queryable<K extends Comparable<K>, V> {
    Iterator<KeyValue<K, V>> getSortedKeyValueIterator(Order order) throws InternalOperationException;
    default Iterator<V> getSortedValueIterator(Order order) throws InternalOperationException {
        return IteratorUtils.modifyNext(getSortedKeyValueIterator(order), KeyValue::value);
    }
    default Iterator<K> getSortedKeyIterator(Order order) throws InternalOperationException {
        return IteratorUtils.modifyNext(getSortedKeyValueIterator(order), KeyValue::key);
    }
    Iterator<V> getGreaterThan(K k, Order order) throws InternalOperationException;
    Iterator<V> getGreaterThanEqual(K k, Order order) throws InternalOperationException;
    Iterator<V> getLessThan(K k, Order order) throws InternalOperationException;
    Iterator<V> getLessThanEqual(K k, Order order) throws InternalOperationException;
    Iterator<V> getEqual(K k, Order order) throws InternalOperationException;
    Iterator<V> getNotEqual(K k, Order order) throws InternalOperationException;
    Iterator<V> getNulls(Order order);
}
