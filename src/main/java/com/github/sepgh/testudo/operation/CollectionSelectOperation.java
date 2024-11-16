package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.storage.db.DBObject;

import java.util.Iterator;

public interface CollectionSelectOperation {
    CollectionSelectOperation query(Query query);
    Iterator<DBObject> execute();
    <T> Iterator<T> execute(Class<T> clazz);
    long count();
}
