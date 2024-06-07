package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface IndexIOSession {
    Optional<BaseClusterTreeNode> getRoot() throws ExecutionException, InterruptedException;
    IndexStorageManager.NodeData write(BaseClusterTreeNode node) throws IOException, ExecutionException, InterruptedException;
    BaseClusterTreeNode read(Pointer pointer) throws ExecutionException, InterruptedException, IOException;
    void update(BaseClusterTreeNode... nodes) throws IOException, InterruptedException;
    void remove(BaseClusterTreeNode node) throws ExecutionException, InterruptedException;
    IndexStorageManager getIndexStorageManager();
    void commit() throws InterruptedException, IOException, ExecutionException;
}
