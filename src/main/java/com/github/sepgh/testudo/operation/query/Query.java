package com.github.sepgh.testudo.operation.query;

import com.github.sepgh.testudo.operation.CollectionIndexProvider;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    public <V extends Number> List<V> execute(CollectionIndexProvider collectionIndexProvider) {
        // Initialize iterator based on conditions and sorting
        Iterator<V> iterator = (Iterator<V>) rootCondition.evaluate(
                collectionIndexProvider,
                sortField == null ? Order.DEFAULT : sortField.order()
        );

        // Apply offset and limit on the results
        List<V> results = new ArrayList<>();
        while (iterator.hasNext() && results.size() < limit) {

            V next = iterator.next();

            if (offset > 0) {
                offset--;
                continue;
            }
            results.add(next);
        }

        return results;
    }
}
