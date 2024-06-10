package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.data.NodeInnerObj;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TableLevelAsyncIndexManagerDecorator<K extends Comparable<K>, V extends Comparable<V>> extends IndexManagerDecorator<K, V> {
    private final Map<Integer, LockManager> lockManagerPool = new ConcurrentHashMap<>();
    public TableLevelAsyncIndexManagerDecorator(IndexManager<K, V> indexManager) {
        super(indexManager);
    }

    protected LockManager getLockManager(int table){
        return lockManagerPool.computeIfAbsent(table, integer -> new LockManager());
    }

    @Override
    public AbstractTreeNode<K> addIndex(int table, K identifier, V value) throws ExecutionException, InterruptedException, IOException, NodeInnerObj.InvalidValueForNodeInnerObj {
        LockManager lockManager = getLockManager(table);
        lockManager.writeLock.lock();
        try {
            return super.addIndex(table, identifier, value);
        } finally {
            lockManager.writeLock.unlock();
        }
    }

    @Override
    public Optional<V> getIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
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
