package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBLevelAsyncIndexManagerDecorator<K extends Comparable<K>, V extends Comparable<V>> extends IndexManagerDecorator<K, V> {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    public DBLevelAsyncIndexManagerDecorator(IndexManager<K, V> indexManager) {
        super(indexManager);
    }
    
    @Override
    public AbstractTreeNode<K> addIndex(int table, K identifier, V value) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException {
        writeLock.lock();
        try {
            return super.addIndex(table, identifier, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Optional<V> getIndex(int table, K identifier) throws InternalOperationException {
        readLock.lock();
        try {
            return super.getIndex(table, identifier);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean removeIndex(int table, K identifier) throws InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        writeLock.lock();
        try {
            return super.removeIndex(table, identifier);
        } finally {
            writeLock.unlock();
        }
    }

}
