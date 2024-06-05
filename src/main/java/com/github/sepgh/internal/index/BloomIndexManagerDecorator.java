package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class BloomIndexManagerDecorator extends IndexManagerDecorator {

    private final IndexStorageManager indexStorageManager;

    public BloomIndexManagerDecorator(IndexManager indexManager, int size, double tolerance, IndexStorageManager indexStorageManager) {
        super(indexManager);
        this.indexStorageManager = indexStorageManager;

    }

    @Override
    public BaseTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        return super.addIndex(table, identifier, pointer);
    }

    @Override
    public Optional<Pointer> getIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        return super.getIndex(table, identifier);
    }

    @Override
    public boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        return super.removeIndex(table, identifier);
    }
}
