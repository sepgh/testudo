package com.github.sepgh.internal.storage.session;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface IndexIOSession {
    Optional<BaseTreeNode> getRoot() throws ExecutionException, InterruptedException;
    IndexStorageManager.NodeData write(BaseTreeNode node) throws IOException, ExecutionException, InterruptedException;
    BaseTreeNode read(Pointer pointer) throws ExecutionException, InterruptedException;
    void update(BaseTreeNode... nodes) throws IOException, InterruptedException;
    void remove(BaseTreeNode node) throws ExecutionException, InterruptedException;
    IndexStorageManager getIndexStorageManager();
    void commit() throws InterruptedException, IOException;
}
