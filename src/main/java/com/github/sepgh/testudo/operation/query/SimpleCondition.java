package com.github.sepgh.testudo.operation.query;

import com.github.sepgh.testudo.operation.CollectionIndexProvider;
import com.github.sepgh.testudo.scheme.Scheme;
import lombok.SneakyThrows;

import java.util.Iterator;

public class SimpleCondition<K extends Comparable<K>> implements Condition {
    private final Scheme.Field field;
    private final Operation operation;
    private final K value;

    public SimpleCondition(Scheme.Collection collection, String field, Operation operation, K value) {
        this.field = collection.getFields().stream().filter(f -> f.getName().equals(field)).findFirst().orElse(null);
        if (field == null) {
            throw new IllegalArgumentException("field not found");
        }
        this.operation = operation;
        this.value = value;
    }

    // Todo: support more operations
    @SneakyThrows
    @Override
    public <V extends Number & Comparable<V>> Iterator<V> evaluate(CollectionIndexProvider collectionIndexProvider, Order order) {
        Queryable<K, V> kvQueryable;
        if (field.isIndexUnique()) {
            kvQueryable = (Queryable<K, V>) collectionIndexProvider.getUniqueIndexManager(field);
        } else {
            kvQueryable = (Queryable<K, V>) collectionIndexProvider.getDuplicateIndexManager(field);
        }
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
        return field.getName();
    }
}
