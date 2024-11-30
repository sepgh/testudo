package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.storage.db.DBObject;

import java.util.Iterator;

public interface CollectionSelectOperation<T extends Number & Comparable<T>> {
    CollectionSelectOperation<T> fields(String... fields);
    CollectionSelectOperation<T> query(Query query);
    Query getQuery();
    Iterator<DBObject> execute();
    <V> Iterator<V> execute(Class<V> clazz);
    long count();
}
