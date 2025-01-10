package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;

import java.util.Iterator;

public class NullableDuplicateQueryableIndex<K extends Comparable<K>, V extends Number & Comparable<V>> extends DuplicateQueryableIndexDecorator<K, V> {
    private final NullableIndexManager<V> nullableIndexManager;

    public NullableDuplicateQueryableIndex(DuplicateQueryableIndex<K, V> decorated, DatabaseStorageManager storageManager, IndexHeaderManager indexHeaderManager, IndexBinaryObjectFactory<V> vIndexBinaryObjectFactory) {
        super(decorated);
        this.nullableIndexManager = new NullableIndexManager<>(storageManager, indexHeaderManager, vIndexBinaryObjectFactory, getIndexId());
    }


    @Override
    public boolean isNull(V value) {
        return this.nullableIndexManager.isNull(value);
    }

    @Override
    public void addNull(V value) throws InternalOperationException {
        this.nullableIndexManager.addNull(value);
    }

    @Override
    public void removeNull(V value) throws InternalOperationException {
        this.nullableIndexManager.removeNull(value);
    }

    @Override
    public Iterator<V> getNullIndexes(Order order) {
        return this.nullableIndexManager.getNulls(order);
    }

    @Override
    public Iterator<V> getNulls(Order order) {
        return this.getNullIndexes(order);
    }
}
