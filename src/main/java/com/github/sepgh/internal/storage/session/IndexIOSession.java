package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.exception.InternalOperationException;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;

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
