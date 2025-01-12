package com.github.sepgh.testudo.operation.query;

import com.github.sepgh.testudo.operation.CollectionIndexProvider;
import com.google.common.base.Preconditions;

import java.util.Iterator;

public class NullCondition<K extends Comparable<K>> implements Condition {
    private final String field;

    public NullCondition(String field) {
        Preconditions.checkNotNull(field);
        this.field = field;
    }

    @Override
    public <V extends Number & Comparable<V>> Iterator<V> evaluate(CollectionIndexProvider collectionIndexProvider, Order order) {
        @SuppressWarnings("unchecked")
        Queryable<K, V> kvQueryable = (Queryable<K, V>) collectionIndexProvider.getQueryableIndex(field);
        return kvQueryable.getNulls(Order.DEFAULT);
    }

    @Override
    public String getField() {
        return field;
    }
}
