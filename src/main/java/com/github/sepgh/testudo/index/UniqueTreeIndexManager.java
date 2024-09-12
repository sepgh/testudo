package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObject;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Optional;

public interface UniqueTreeIndexManager<K extends Comparable<K>, V> {
    AbstractTreeNode<K> addIndex(K identifier, V value) throws IndexExistsException, InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject;
    AbstractTreeNode<K> updateIndex(K identifier, V value) throws IndexExistsException, InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IndexMissingException;
    Optional<V> getIndex(K identifier) throws InternalOperationException;
    boolean removeIndex(K identifier) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject;
    int size() throws InternalOperationException;
    LockableIterator<KeyValue<K, V>> getSortedIterator() throws InternalOperationException;
    void purgeIndex();
    int getIndexId();
}
