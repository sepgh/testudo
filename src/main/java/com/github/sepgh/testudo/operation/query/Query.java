package com.github.sepgh.testudo.operation.query;

import com.github.sepgh.testudo.operation.CollectionIndexProvider;
import com.github.sepgh.testudo.utils.IteratorUtils;
import lombok.SneakyThrows;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

public class Query {
    private Condition rootCondition = null;
    private SortField sortField;
    private int offset = 0;
    private int limit = Integer.MAX_VALUE;

    public Query where(Condition condition) {
        if (rootCondition == null) {
            rootCondition = condition;
        } else {
            ((CompositeCondition) rootCondition).addCondition(condition);
        }
        return this;
    }

    public Query and(Condition condition) {
        if (rootCondition == null) {
            throw new IllegalStateException("Root condition is null");
        }
        rootCondition = new CompositeCondition(CompositeCondition.CompositeOperator.AND, rootCondition, condition);
        return this;
    }

    public Query or(Condition condition) {
        if (rootCondition == null) {
            throw new IllegalStateException("Root condition is null");
        }
        rootCondition = new CompositeCondition(CompositeCondition.CompositeOperator.OR, rootCondition, condition);
        return this;
    }

    public Query sort(SortField sortField) {
        this.sortField = sortField;
        return this;
    }

    public Query offset(int offset) {
        this.offset = offset;
        return this;
    }

    public Query limit(int limit) {
        this.limit = limit;
        return this;
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public <V extends Number & Comparable<V>> Iterator<V> execute(CollectionIndexProvider collectionIndexProvider) {
        Iterator<V> iterator;

        if (rootCondition != null) {
            // Initialize iterator based on conditions and sorting
            iterator = rootCondition.evaluate(
                    collectionIndexProvider,
                    Order.DEFAULT
            );

            if (sortField != null) {
                Iterator<V> sortedValueIterator = (Iterator<V>) collectionIndexProvider.getQueryableIndex(sortField.field()).getSortedValueIterator(sortField.order());
                iterator = new SortedIterator<>(
                        iterator,
                        sortedValueIterator
                );
            }
        } else if (sortField != null) {
            iterator = (Iterator<V>) collectionIndexProvider.getQueryableIndex(sortField.field()).getSortedValueIterator(sortField.order());
        } else {
            iterator = IteratorUtils.modifyNext(
                    collectionIndexProvider.getClusterIndexManager().getSortedIterator(Order.DEFAULT),
                    pointerKeyValue -> (V) pointerKeyValue.key()
            );
        }

        // Apply offset and limit on the results
        while (iterator.hasNext() && offset > 0) {
            iterator.next();
            offset--;
        }

        if (!iterator.hasNext() || limit <= 0) {
            return Collections.emptyIterator();
        }

        final Iterator<V> finalIterator = iterator;
        final AtomicInteger counter = new AtomicInteger(0);

        return new Iterator<V>() {
            @Override
            public boolean hasNext() {
                return finalIterator.hasNext() && counter.get() < limit;
            }

            @Override
            public V next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                V next = finalIterator.next();
                counter.incrementAndGet();
                return next;
            }
        };
    }
}
