package com.github.sepgh.internal.index;

import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.data.ImmutableBinaryObjectWrapper;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBLevelAsyncIndexManagerDecorator<K extends Comparable<K>, V extends Comparable<V>> extends IndexManagerDecorator<K, V> {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    public DBLevelAsyncIndexManagerDecorator(IndexManager<K, V> indexManager) {
        super(indexManager);
    }
    
    @Override
    public AbstractTreeNode<K> addIndex(int table, K identifier, V value) throws ExecutionException, InterruptedException, IOException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        writeLock.lock();
        try {
            return super.addIndex(table, identifier, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Optional<V> getIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        readLock.lock();
        try {
            return super.getIndex(table, identifier);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean removeIndex(int table, K identifier) throws ExecutionException, InterruptedException, IOException {
        writeLock.lock();
        try {
            return super.removeIndex(table, identifier);
        } finally {
            writeLock.unlock();
        }
    }

}
