package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.storage.db.DBObject;

import java.util.Iterator;

public interface CollectionSelectOperation {
    CollectionSelectOperation fields(String... fields);
    CollectionSelectOperation query(Query query);
    Query getQuery();
    <V extends Number & Comparable<V>> Iterator<DBObject> execute();
    <V extends Number & Comparable<V>, T> Iterator<T> execute(Class<T> clazz);
    long count();
}
