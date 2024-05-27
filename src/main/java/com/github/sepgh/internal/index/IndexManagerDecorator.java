package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.BaseTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class IndexManagerDecorator implements IndexManager {
    private final IndexManager indexManager;

    public IndexManagerDecorator(IndexManager indexManager) {
        this.indexManager = indexManager;
    }

    public BaseTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException{
        return this.indexManager.addIndex(table, identifier, pointer);
    }
    public Optional<Pointer> getIndex(int table, long identifier) throws ExecutionException, InterruptedException {
        return this.indexManager.getIndex(table, identifier);
    }

    public boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        return this.indexManager.removeIndex(table, identifier);
    }
}
