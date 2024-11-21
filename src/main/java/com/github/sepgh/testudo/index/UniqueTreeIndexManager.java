package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Optional;

public interface UniqueTreeIndexManager<K extends Comparable<K>, V> {
    AbstractTreeNode<K> addIndex(K identifier, V value) throws IndexExistsException, InternalOperationException;
    AbstractTreeNode<K> updateIndex(K identifier, V value) throws InternalOperationException, IndexMissingException;
    Optional<V> getIndex(K identifier) throws InternalOperationException;
    boolean removeIndex(K identifier) throws InternalOperationException;
    int size() throws InternalOperationException;
    LockableIterator<KeyValue<K, V>> getSortedIterator(Order order) throws InternalOperationException;
    void purgeIndex();
    int getIndexId();
    boolean supportIncrement();
    K nextKey() throws InternalOperationException;
}
