package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBLevelAsyncIndexManagerDecorator extends IndexManagerDecorator {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    public DBLevelAsyncIndexManagerDecorator(IndexManager indexManager) {
        super(indexManager);
    }
    
    @Override
    public BaseClusterTreeNode addIndex(int table, long identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        writeLock.lock();
        try {
            return super.addIndex(table, identifier, pointer);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Optional<Pointer> getIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        readLock.lock();
        try {
            return super.getIndex(table, identifier);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean removeIndex(int table, long identifier) throws ExecutionException, InterruptedException, IOException {
        writeLock.lock();
        try {
            return super.removeIndex(table, identifier);
        } finally {
            writeLock.unlock();
        }
    }

}
