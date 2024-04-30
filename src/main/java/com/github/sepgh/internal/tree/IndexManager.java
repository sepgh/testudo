package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.tree.exception.IllegalNodeAccess;
import com.github.sepgh.internal.tree.node.BaseTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface IndexManager {
    BaseTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IllegalNodeAccess, IOException;
    CompletableFuture<Optional<BaseTreeNode>> getIndex(int table, long identifier);
    CompletableFuture<Void> removeIndex(int table, long identifier);
}
