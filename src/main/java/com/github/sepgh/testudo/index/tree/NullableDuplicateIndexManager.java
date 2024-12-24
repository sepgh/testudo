package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.index.DuplicateIndexManager;
import com.github.sepgh.testudo.index.DuplicateIndexManagerDecorator;
import com.github.sepgh.testudo.index.NullableIndexManager;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;

import java.util.Iterator;

public class NullableDuplicateIndexManager<K extends Comparable<K>, V extends Number & Comparable<V>> extends DuplicateIndexManagerDecorator<K, V> {
    private final NullableIndexManager<V> nullableIndexManager;

    public NullableDuplicateIndexManager(DuplicateIndexManager<K, V> decorated, DatabaseStorageManager storageManager, IndexHeaderManager indexHeaderManager, IndexBinaryObjectFactory<V> vIndexBinaryObjectFactory) {
        super(decorated);
        this.nullableIndexManager = new NullableIndexManager<>(storageManager, indexHeaderManager, vIndexBinaryObjectFactory, getIndexId());
    }


    @Override
    public boolean isNull(V value) {
        return this.nullableIndexManager.isNull(value);
    }

    @Override
    public void addNull(V value) {
        this.nullableIndexManager.addNull(value);
    }

    @Override
    public void removeNull(V value) {
        this.nullableIndexManager.removeNull(value);
    }

    @Override
    public Iterator<V> getNulls(Order order) {
        return this.nullableIndexManager.getNulls(order);
    }

    @Override
    public Iterator<V> getNotNulls(Order order) {
        return this.nullableIndexManager.getNotNulls(order);
    }

}
