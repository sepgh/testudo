package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface IndexIOSession<K extends Comparable<K>> {
    Optional<AbstractTreeNode<K>> getRoot() throws ExecutionException, InterruptedException;
    IndexStorageManager.NodeData write(AbstractTreeNode<K> node) throws IOException, ExecutionException, InterruptedException;
    AbstractTreeNode<K> read(Pointer pointer) throws ExecutionException, InterruptedException, IOException;
    void update(AbstractTreeNode<K> node) throws IOException, InterruptedException, ExecutionException;
    void remove(AbstractTreeNode<K> node) throws ExecutionException, InterruptedException;
    IndexStorageManager getIndexStorageManager();
    void commit() throws InterruptedException, IOException, ExecutionException;
}
