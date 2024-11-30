package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.ReaderWriterLock;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Optional;

// Note: I'm guessing this is useless! We need to lock multiple addIndex() operations and this is not the place for that!
public class AsyncUniqueTreeIndexManagerDecorator<K extends Comparable<K>, V> extends UniqueTreeIndexManagerDecorator<K, V> {
    private final ReaderWriterLock ReaderWriterLock;

    public AsyncUniqueTreeIndexManagerDecorator(UniqueTreeIndexManager<K, V> uniqueTreeIndexManager, ReaderWriterLock ReaderWriterLock) {
        super(uniqueTreeIndexManager);
        this.ReaderWriterLock = ReaderWriterLock;
    }
    
    @Override
    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException, IndexExistsException {
        ReaderWriterLock.getWriteLock().lock();
        try {
            return super.addIndex(identifier, value);
        } finally {
            ReaderWriterLock.getWriteLock().unlock();
        }
    }

    @Override
    public Optional<V> getIndex(K identifier) throws InternalOperationException {
        ReaderWriterLock.getReadLock().lock();
        try {
            return super.getIndex(identifier);
        } finally {
            ReaderWriterLock.getReadLock().unlock();
        }
    }

    @Override
    public boolean removeIndex(K identifier) throws InternalOperationException {
        ReaderWriterLock.getWriteLock().lock();
        try {
            return super.removeIndex(identifier);
        } finally {
            ReaderWriterLock.getWriteLock().unlock();
        }
    }

    @Override
    public AbstractTreeNode<K> updateIndex(K identifier, V value) throws InternalOperationException, IndexMissingException {
        ReaderWriterLock.getWriteLock().lock();
        try {
            return super.updateIndex(identifier, value);
        } finally {
            ReaderWriterLock.getWriteLock().unlock();
        }
    }

    @Override
    public void purgeIndex() {
        ReaderWriterLock.getWriteLock().lock();
        try {
            super.purgeIndex();
        } finally {
            ReaderWriterLock.getWriteLock().unlock();
        }
    }

    @Override
    public LockableIterator<KeyValue<K, V>> getSortedIterator(Order order) throws InternalOperationException {
        LockableIterator<KeyValue<K, V>> iterator = super.getSortedIterator(order);
        return new LockableIterator<>() {
            @Override
            public void lock() {
                ReaderWriterLock.getReadLock().lock();
            }

            @Override
            public void unlock() {
                ReaderWriterLock.getReadLock().unlock();
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
