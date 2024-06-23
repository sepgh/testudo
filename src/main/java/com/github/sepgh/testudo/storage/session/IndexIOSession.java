package com.github.sepgh.testudo.storage.session;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.storage.IndexStorageManager;

import java.util.Optional;

public interface IndexIOSession<K extends Comparable<K>> {
    Optional<AbstractTreeNode<K>> getRoot() throws InternalOperationException;
    IndexStorageManager.NodeData write(AbstractTreeNode<K> node) throws InternalOperationException;
    AbstractTreeNode<K> read(Pointer pointer) throws InternalOperationException;
    void update(AbstractTreeNode<K> node) throws InternalOperationException;
    void remove(AbstractTreeNode<K> node) throws InternalOperationException;
    IndexStorageManager getIndexStorageManager();
    void commit() throws InternalOperationException;
}
