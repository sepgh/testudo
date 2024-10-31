package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.IndexMissingException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.Optional;

// Note: I'm guessing this is useless! We need to lock multiple addIndex() operations and this is not the place for that!
public class AsyncUniqueTreeIndexManagerDecorator<K extends Comparable<K>, V> extends UniqueTreeIndexManagerDecorator<K, V> {
    private final IndexManagerLock indexManagerLock;

    public AsyncUniqueTreeIndexManagerDecorator(UniqueTreeIndexManager<K, V> uniqueTreeIndexManager, IndexManagerLock indexManagerLock) {
        super(uniqueTreeIndexManager);
        this.indexManagerLock = indexManagerLock;
    }
    
    @Override
    public AbstractTreeNode<K> addIndex(K identifier, V value) throws InternalOperationException, IndexExistsException {
        indexManagerLock.getWriteLock().lock();
        try {
            return super.addIndex(identifier, value);
        } finally {
            indexManagerLock.getWriteLock().unlock();
        }
    }

    @Override
    public Optional<V> getIndex(K identifier) throws InternalOperationException {
        indexManagerLock.getReadLock().lock();
        try {
            return super.getIndex(identifier);
        } finally {
            indexManagerLock.getReadLock().unlock();
        }
    }

    @Override
    public boolean removeIndex(K identifier) throws InternalOperationException {
        indexManagerLock.getWriteLock().lock();
        try {
            return super.removeIndex(identifier);
        } finally {
            indexManagerLock.getWriteLock().unlock();
        }
    }

    @Override
    public AbstractTreeNode<K> updateIndex(K identifier, V value) throws InternalOperationException, IndexMissingException {
        indexManagerLock.getWriteLock().lock();
        try {
            return super.updateIndex(identifier, value);
        } finally {
            indexManagerLock.getWriteLock().unlock();
        }
    }

    @Override
    public void purgeIndex() {
        indexManagerLock.getWriteLock().lock();
        try {
            super.purgeIndex();
        } finally {
            indexManagerLock.getWriteLock().unlock();
        }
    }

    @Override
    public LockableIterator<KeyValue<K, V>> getSortedIterator(Order order) throws InternalOperationException {
        LockableIterator<KeyValue<K, V>> iterator = super.getSortedIterator(order);
        return new LockableIterator<>() {
            @Override
            public void lock() {
                indexManagerLock.getReadLock().lock();
            }

            @Override
            public void unlock() {
                indexManagerLock.getReadLock().unlock();
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
