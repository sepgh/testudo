package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.io.IOException;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface DuplicateIndexManager<K extends Comparable<K>, V extends Number & Comparable<V>> {
    // Todo: throw signature?
    boolean addIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException;
    Optional<ListIterator<V>> getIndex(K identifier) throws InternalOperationException;
    Optional<ListIterator<V>> getIndex(K identifier, Order order) throws InternalOperationException;
    boolean removeIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException;
    int size() throws InternalOperationException;
    LockableIterator<KeyValue<K, ListIterator<V>>> getSortedIterator(Order order) throws InternalOperationException;
    void purgeIndex();
    int getIndexId();
}
