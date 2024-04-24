package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.IndexFileManager;

import java.util.Optional;
import java.util.concurrent.Future;

public class BTreeIndexManager implements IndexManager {

    private final IndexFileManager indexFileManager;

    public BTreeIndexManager(IndexFileManager indexFileManager){
        this.indexFileManager = indexFileManager;
    }

    @Override
    public Future<Pointer> addIndex(long identifier, Pointer pointer) {
        return null;
    }

    @Override
    public Future<Optional<Pointer>> getIndex(long identifier) {
        return null;
    }

    @Override
    public Future<Void> removeIndex(long identifier) {
        return null;
    }
}
