package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.FileIndexStorageManager;

import java.util.Optional;
import java.util.concurrent.Future;

public class BTreeIndexManager implements IndexManager {

    private final FileIndexStorageManager fileIndexStorageManager;

    public BTreeIndexManager(FileIndexStorageManager fileIndexStorageManager){
        this.fileIndexStorageManager = fileIndexStorageManager;
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
