package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public interface IndexManager<K extends Comparable<K>, V extends Comparable<V>> {
    AbstractTreeNode<K> addIndex(int table, K identifier, V value) throws ExecutionException, InterruptedException, IOException;
    Optional<V> getIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException;
    boolean removeIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException;
    int size(int table) throws InterruptedException, ExecutionException, IOException;
}
