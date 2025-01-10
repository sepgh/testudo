package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.ListIterator;
import java.util.Optional;

public interface DuplicateIndexManager<K extends Comparable<K>, V extends Number & Comparable<V>> extends NullableIndex<V> {
    // Todo: throw signature?
    boolean addIndex(K identifier, V value) throws InternalOperationException, DeserializationException;
    Optional<ListIterator<V>> getIndex(K identifier) throws InternalOperationException;
    Optional<ListIterator<V>> getIndex(K identifier, Order order) throws InternalOperationException;
    boolean removeIndex(K identifier, V value) throws InternalOperationException, DeserializationException;
    int size() throws InternalOperationException;
    LockableIterator<KeyValue<K, ListIterator<V>>> getSortedIterator(Order order) throws InternalOperationException;
    void purgeIndex() throws InternalOperationException;
    int getIndexId();
    UniqueTreeIndexManager<K, Pointer> getInnerIndexManager();
}
