package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObject;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class AsyncUniqueTreeIndexManagerDecorator<K extends Comparable<K>, V> extends UniqueTreeIndexManagerDecorator<K, V> {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    public AsyncUniqueTreeIndexManagerDecorator(UniqueTreeIndexManager<K, V> uniqueTreeIndexManager) {
        super(uniqueTreeIndexManager);
    }
    
    @Override
    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IndexExistsException {
        writeLock.lock();
        try {
            return super.addIndex(identifier, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Optional<V> getIndex(K identifier) throws InternalOperationException {
        readLock.lock();
        try {
            return super.getIndex(identifier);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean removeIndex(K identifier) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject {
        writeLock.lock();
        try {
            return super.removeIndex(identifier);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public AbstractTreeNode<K> updateIndex(K identifier, V value) throws InternalOperationException, IndexBinaryObject.InvalidIndexBinaryObject, IndexMissingException {
        writeLock.lock();
        try {
            return super.updateIndex(identifier, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void purgeIndex() {
        writeLock.lock();
        try {
            super.purgeIndex();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public LockableIterator<KeyValue<K, V>> getSortedIterator() throws InternalOperationException {
        LockableIterator<KeyValue<K, V>> iterator = super.getSortedIterator();
        return new LockableIterator<>() {
            @Override
            public void lock() {
                readLock.lock();
            }

            @Override
            public void unlock() {
                readLock.unlock();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<K, V> next() {
                return iterator.next();
            }
        };

    }
}
