package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.tree.node.AbstractTreeNode;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface IndexManager {
    CompletableFuture<AbstractTreeNode> addIndex(long identifier, Pointer pointer);
    CompletableFuture<Optional<AbstractTreeNode>> getIndex(long identifier);
    CompletableFuture<Void> removeIndex(long identifier);
}
