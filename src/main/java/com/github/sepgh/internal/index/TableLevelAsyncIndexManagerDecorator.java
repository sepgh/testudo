package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TableLevelAsyncIndexManagerDecorator<K extends Comparable<K>> extends IndexManagerDecorator<K> {
    private final Map<Integer, LockManager> lockManagerPool = new ConcurrentHashMap<>();
    public TableLevelAsyncIndexManagerDecorator(IndexManager<K> indexManager) {
        super(indexManager);
    }

    protected LockManager getLockManager(int table){
        return lockManagerPool.computeIfAbsent(table, integer -> new LockManager());
    }

    @Override
    public BaseClusterTreeNode<K> addIndex(int table, K identifier, Pointer pointer) throws ExecutionException, InterruptedException, IOException {
        LockManager lockManager = getLockManager(table);
        lockManager.writeLock.lock();
        try {
            return super.addIndex(table, identifier, pointer);
        } finally {
            lockManager.writeLock.unlock();
        }
    }

    @Override
    public Optional<Pointer> getIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        LockManager lockManager = getLockManager(table);
        lockManager.readLock.lock();
        try {
            return super.getIndex(table, identifier);
        } finally {
            lockManager.readLock.unlock();
        }
    }

    @Override
    public boolean removeIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        LockManager lockManager = getLockManager(table);
        lockManager.writeLock.lock();
        try {
            return super.removeIndex(table, identifier);
        } finally {
            lockManager.writeLock.unlock();
        }
    }

    public static class LockManager {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    }

}
