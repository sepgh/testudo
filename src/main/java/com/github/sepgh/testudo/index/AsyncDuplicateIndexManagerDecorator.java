package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.LockableIterator;
import com.github.sepgh.testudo.utils.ReaderWriterLock;

import java.io.IOException;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class AsyncDuplicateIndexManagerDecorator<K extends Comparable<K>, V extends Number & Comparable<V>> extends DuplicateIndexManagerDecorator<K,V> {
    private final ReaderWriterLock ReaderWriterLock;

    public AsyncDuplicateIndexManagerDecorator(DuplicateIndexManager<K, V> decorated, ReaderWriterLock ReaderWriterLock) {
        super(decorated);
        this.ReaderWriterLock = ReaderWriterLock;
    }

    @Override
    public boolean addIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException {
        try {
            this.ReaderWriterLock.getWriteLock().lock();
            return super.addIndex(identifier, value);
        } finally {
            this.ReaderWriterLock.getWriteLock().unlock();
        }
    }

    @Override
    public Optional<ListIterator<V>> getIndex(K identifier) throws InternalOperationException {
        try {
            this.ReaderWriterLock.getReadLock().lock();
            return super.getIndex(identifier);
        } finally {
            this.ReaderWriterLock.getReadLock().unlock();
        }
    }

    @Override
    public boolean removeIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException {
        try {
            this.ReaderWriterLock.getWriteLock().lock();
            return super.removeIndex(identifier, value);
        } finally {
            this.ReaderWriterLock.getWriteLock().unlock();
        }
    }

    @Override
    public int size() throws InternalOperationException {
        try {
            this.ReaderWriterLock.getReadLock().lock();
            return super.size();
        } finally {
            this.ReaderWriterLock.getReadLock().unlock();
        }
    }

    @Override
    public LockableIterator<KeyValue<K, ListIterator<V>>> getSortedIterator(Order order) throws InternalOperationException {
        LockableIterator<KeyValue<K, ListIterator<V>>> sortedIterator = super.getSortedIterator(order);
        return new LockableIterator<KeyValue<K, ListIterator<V>>>() {
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
        ReaderWriterLock.getWriteLock().lock();
        try {
            super.purgeIndex();
        } finally {
            ReaderWriterLock.getWriteLock().unlock();
        }
    }
}
