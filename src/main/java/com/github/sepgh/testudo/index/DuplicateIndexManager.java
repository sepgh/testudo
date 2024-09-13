package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObject;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.io.IOException;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface DuplicateIndexManager<K extends Comparable<K>, V extends Number & Comparable<V>> {
    boolean addIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IOException, ExecutionException, InterruptedException;
    Optional<ListIterator<V>> getIndex(K identifier) throws InternalOperationException;
    boolean removeIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IOException, ExecutionException, InterruptedException;
    int size() throws InternalOperationException;
    LockableIterator<KeyValue<K, ListIterator<V>>> getSortedIterator() throws InternalOperationException;
    void purgeIndex();
    int getIndexId();
}
