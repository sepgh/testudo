package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.List;

public interface CollectionSelectOperation<T extends Number & Comparable<T>> {
    CollectionSelectOperation<T> fields(String... fields);
    CollectionSelectOperation<T> query(Query query);
    Query getQuery();
    LockableIterator<DBObject> execute();
    <V> LockableIterator<V> execute(Class<V> clazz);
    <V> List<V> asList(Class<V> clazz);
    long count();
}
