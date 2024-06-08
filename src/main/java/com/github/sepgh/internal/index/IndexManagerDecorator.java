package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class IndexManagerDecorator<K extends Comparable<K>> implements IndexManager<K> {
    private final IndexManager<K> indexManager;

    public IndexManagerDecorator(IndexManager<K> indexManager) {
        this.indexManager = indexManager;
    }

    public BaseClusterTreeNode<K> addIndex(int table, K identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException{
        return this.indexManager.addIndex(table, identifier, pointer);
    }
    public Optional<Pointer> getIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        return this.indexManager.getIndex(table, identifier);
    }

    public boolean removeIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        return this.indexManager.removeIndex(table, identifier);
    }

    @Override
    public int size(int table) throws ExecutionException, InterruptedException {
        try {
            return this.indexManager.size(table);
        } catch (IOException e) {
            return 0;
        }
    }
}
