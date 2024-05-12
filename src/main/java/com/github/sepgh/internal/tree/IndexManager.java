package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.tree.node.BaseTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface IndexManager {
    BaseTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException;
    Optional<Pointer> getIndex(int table, long identifier) throws ExecutionException, InterruptedException;
    boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException;
}
