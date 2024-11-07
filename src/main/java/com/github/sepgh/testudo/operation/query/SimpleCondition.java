package com.github.sepgh.testudo.operation.query;

import com.github.sepgh.testudo.operation.CollectionIndexProvider;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;

import java.util.Iterator;

public class SimpleCondition<K extends Comparable<K>> implements Condition {
    private final String field;
    private final Operation operation;
    private final K value;

    public SimpleCondition(String field, Operation operation, K value) {
        Preconditions.checkNotNull(field);
        Preconditions.checkNotNull(operation);
        Preconditions.checkNotNull(value);
        this.field = field;
        this.operation = operation;
        this.value = value;
    }

    // Todo: support more operations
    @SneakyThrows
    @Override
    public <V extends Number & Comparable<V>> Iterator<V> evaluate(CollectionIndexProvider collectionIndexProvider, Order order) {
        @SuppressWarnings("unchecked")
        Queryable<K, V> kvQueryable = (Queryable<K, V>) collectionIndexProvider.getQueryableIndex(field);

        return switch (operation) {
            case EQ -> kvQueryable.getEqual(this.value, order);
            case GT -> kvQueryable.getGreaterThan(this.value, order);
            case GTE -> kvQueryable.getGreaterThanEqual(this.value, order);
            case LT -> kvQueryable.getLessThan(this.value, order);
            case LTE -> kvQueryable.getLessThanEqual(this.value, order);
        };
    }

    @Override
    public String getField() {
        return field;
    }
}
