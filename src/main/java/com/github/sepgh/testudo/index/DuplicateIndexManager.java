package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObject;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.ListIterator;
import java.util.Optional;

public interface DuplicateIndexManager<K extends Comparable<K>, V extends Number & Comparable<V>> {
    boolean addIndex(K identifier, V value) throws IndexExistsException, InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject;
    Optional<ListIterator<V>> getIndex(K identifier) throws InternalOperationException;
    boolean removeIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject;
    int size() throws InternalOperationException;
    LockableIterator<KeyValue<K, ListIterator<V>>> getSortedIterator() throws InternalOperationException;
    void purgeIndex();
    int getIndexId();
}
