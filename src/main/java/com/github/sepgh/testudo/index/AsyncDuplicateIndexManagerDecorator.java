package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.io.IOException;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class AsyncDuplicateIndexManagerDecorator<K extends Comparable<K>, V extends Number & Comparable<V>> extends DuplicateIndexManagerDecorator<K,V> {
    private final IndexManagerLock indexManagerLock;

    public AsyncDuplicateIndexManagerDecorator(DuplicateIndexManager<K, V> decorated, IndexManagerLock indexManagerLock) {
        super(decorated);
        this.indexManagerLock = indexManagerLock;
    }

    @Override
    public boolean addIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException {
        try {
            this.indexManagerLock.getWriteLock().lock();
            return super.addIndex(identifier, value);
        } finally {
            this.indexManagerLock.getWriteLock().unlock();
        }
    }

    @Override
    public Optional<ListIterator<V>> getIndex(K identifier) throws InternalOperationException {
        try {
            this.indexManagerLock.getReadLock().lock();
            return super.getIndex(identifier);
        } finally {
            this.indexManagerLock.getReadLock().unlock();
        }
    }

    @Override
    public boolean removeIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException {
        try {
            this.indexManagerLock.getWriteLock().lock();
            return super.removeIndex(identifier, value);
        } finally {
            this.indexManagerLock.getWriteLock().unlock();
        }
    }

    @Override
    public int size() throws InternalOperationException {
        try {
            this.indexManagerLock.getReadLock().lock();
            return super.size();
        } finally {
            this.indexManagerLock.getReadLock().unlock();
        }
    }

    @Override
    public LockableIterator<KeyValue<K, ListIterator<V>>> getSortedIterator(Order order) throws InternalOperationException {
        LockableIterator<KeyValue<K, ListIterator<V>>> sortedIterator = super.getSortedIterator(order);
        return new LockableIterator<KeyValue<K, ListIterator<V>>>() {
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
                return sortedIterator.hasNext();
            }

            @Override
            public KeyValue<K, ListIterator<V>> next() {
                return sortedIterator.next();
            }
        };
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
}
