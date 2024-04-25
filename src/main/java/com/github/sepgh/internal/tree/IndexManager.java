package com.github.sepgh.internal.tree;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface IndexManager {
    CompletableFuture<TreeNode> addIndex(long identifier, Pointer pointer);
    CompletableFuture<Optional<TreeNode>> getIndex(long identifier);
    CompletableFuture<Void> removeIndex(long identifier);
}
