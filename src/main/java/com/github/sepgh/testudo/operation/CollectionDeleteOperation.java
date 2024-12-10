package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.operation.query.Query;

public interface CollectionDeleteOperation<T extends Number & Comparable<T>> {
    CollectionDeleteOperation<T> query(Query query);
    Query getQuery();
    int execute();
}
