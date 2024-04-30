package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.tree.exception.IllegalNodeAccess;
import com.github.sepgh.internal.tree.node.BaseTreeNode;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface IndexManager {
    CompletableFuture<BaseTreeNode> addIndex(long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IllegalNodeAccess;
    CompletableFuture<Optional<BaseTreeNode>> getIndex(long identifier);
    CompletableFuture<Void> removeIndex(long identifier);
}
