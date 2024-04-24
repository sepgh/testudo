package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.storage.IndexFileManager;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Future;

public class BTreeIndexManager implements IndexManager {

    private final IndexFileManager indexFileManager;
    private final String tableName;

    public BTreeIndexManager(Path path, String tableName, String tableName1) {
        this.indexFileManager = new IndexFileManager(path);
        this.tableName = tableName1;
    }

    public BTreeIndexManager(IndexFileManager indexFileManager, String tableName){
        this.indexFileManager = indexFileManager;
        this.tableName = tableName;
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
