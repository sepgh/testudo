package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.io.IOException;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class DuplicateIndexManagerDecorator<K extends Comparable<K>, V extends Number & Comparable<V>> implements DuplicateIndexManager<K, V> {
    protected final DuplicateIndexManager<K, V> decorated;

    public DuplicateIndexManagerDecorator(DuplicateIndexManager<K, V> decorated) {
        this.decorated = decorated;
    }

    @Override
    public boolean addIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException {
        return this.decorated.addIndex(identifier, value);
    }

    @Override
    public Optional<ListIterator<V>> getIndex(K identifier) throws InternalOperationException {
        return this.decorated.getIndex(identifier);
    }

    @Override
    public Optional<ListIterator<V>> getIndex(K identifier, Order order) throws InternalOperationException {
        return this.decorated.getIndex(identifier, order);
    }

    @Override
    public boolean removeIndex(K identifier, V value) throws InternalOperationException, IOException, ExecutionException, InterruptedException {
        return this.decorated.removeIndex(identifier, value);
    }

    @Override
    public int size() throws InternalOperationException {
        return this.decorated.size();
    }

    @Override
    public LockableIterator<KeyValue<K, ListIterator<V>>> getSortedIterator(Order order) throws InternalOperationException {
        return this.decorated.getSortedIterator(order);
    }

    @Override
    public void purgeIndex() {
        this.decorated.purgeIndex();
    }

    @Override
    public int getIndexId() {
        return this.decorated.getIndexId();
    }

    @Override
    public UniqueTreeIndexManager<K, Pointer> getInnerIndexManager() {
        return this.decorated.getInnerIndexManager();
    }
}
