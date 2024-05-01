package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.tree.exception.IllegalNodeAccess;
import com.github.sepgh.internal.tree.node.BaseTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface IndexManager {
    BaseTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IllegalNodeAccess, IOException;
    Optional<Pointer> getIndex(int table, long identifier) throws IOException, ExecutionException, InterruptedException;
    boolean removeIndex(int table, long identifier);
}
