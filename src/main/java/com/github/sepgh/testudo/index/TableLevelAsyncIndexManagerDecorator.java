package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
    public AbstractTreeNode<K> addIndex(int table, K identifier, V value) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException {
        LockManager lockManager = getLockManager(table);
        lockManager.writeLock.lock();
        try {
            return super.addIndex(table, identifier, value);
        } finally {
            lockManager.writeLock.unlock();
        }
    }

    @Override
    public Optional<V> getIndex(int table, K identifier) throws InternalOperationException {
        LockManager lockManager = getLockManager(table);
        lockManager.readLock.lock();
        try {
            return super.getIndex(table, identifier);
        } finally {
            lockManager.readLock.unlock();
        }
    }

    @Override
    public boolean removeIndex(int table, K identifier) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        LockManager lockManager = getLockManager(table);
        lockManager.writeLock.lock();
        try {
            return super.removeIndex(table, identifier);
        } finally {
            lockManager.writeLock.unlock();
        }
    }

    @Override
    public LockableIterator<AbstractLeafTreeNode.KeyValue<K, V>> getSortedIterator(int table) throws InternalOperationException {
        LockableIterator<AbstractLeafTreeNode.KeyValue<K, V>> iterator = super.getSortedIterator(table);
        LockManager lockManager = getLockManager(table);
        return new LockableIterator<AbstractLeafTreeNode.KeyValue<K, V>>() {
            @Override
            public void lock() {
                lockManager.readLock.lock();
            }

            @Override
            public void unlock() {
                lockManager.readLock.unlock();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public AbstractLeafTreeNode.KeyValue<K, V> next() {
                return iterator.next();
            }
        };
    }

    public static class LockManager {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    }

}
