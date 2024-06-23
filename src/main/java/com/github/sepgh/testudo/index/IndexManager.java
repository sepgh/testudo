package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;

import java.util.Optional;

public interface IndexManager<K extends Comparable<K>, V extends Comparable<V>> {
    AbstractTreeNode<K> addIndex(int table, K identifier, V value) throws IndexExistsException, InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue;
    Optional<V> getIndex(int table, K identifier) throws InternalOperationException;
    boolean removeIndex(int table, K identifier) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue;
    int size(int table) throws InternalOperationException;
}
