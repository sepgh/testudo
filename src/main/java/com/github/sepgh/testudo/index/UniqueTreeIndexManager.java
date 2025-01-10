package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Optional;

public interface UniqueTreeIndexManager<K extends Comparable<K>, V> extends NullableIndex<V> {
    AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException;
    AbstractTreeNode<K> addOrUpdateIndex(K identifier, V value) throws InternalOperationException;
    Optional<V> getIndex(K identifier) throws InternalOperationException;
    boolean removeIndex(K identifier) throws InternalOperationException;
    int size() throws InternalOperationException;
    LockableIterator<KeyValue<K, V>> getSortedIterator(Order order) throws InternalOperationException;
    void purgeIndex() throws InternalOperationException;
    int getIndexId();
    boolean supportIncrement();
    K nextKey() throws InternalOperationException, DeserializationException;
}
