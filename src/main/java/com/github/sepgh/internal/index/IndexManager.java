package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.BaseTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface IndexManager {
    BaseTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException;
    Optional<Pointer> getIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException;
    boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException;
    int size(int table) throws InterruptedException, ExecutionException;
}
