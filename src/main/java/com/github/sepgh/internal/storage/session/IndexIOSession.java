package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface IndexIOSession<K extends Comparable<K>> {
    Optional<BaseClusterTreeNode<K>> getRoot() throws ExecutionException, InterruptedException;
    IndexStorageManager.NodeData write(BaseClusterTreeNode<K> node) throws IOException, ExecutionException, InterruptedException;
    BaseClusterTreeNode<K> read(Pointer pointer) throws ExecutionException, InterruptedException, IOException;
    void update(BaseClusterTreeNode<K>... nodes) throws IOException, InterruptedException;
    void remove(BaseClusterTreeNode<K> node) throws ExecutionException, InterruptedException;
    IndexStorageManager getIndexStorageManager();
    void commit() throws InterruptedException, IOException, ExecutionException;
}
