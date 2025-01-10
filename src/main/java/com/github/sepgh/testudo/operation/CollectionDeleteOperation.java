package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.operation.query.Query;

public interface CollectionDeleteOperation<T extends Number & Comparable<T>> {
    CollectionDeleteOperation<T> query(Query query);
    Query getQuery();
    long execute() throws InternalOperationException, DeserializationException;
}
