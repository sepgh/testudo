package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.utils.LockableIterator;

import java.util.List;

public interface CollectionDeleteOperation<T extends Number & Comparable<T>> {
    CollectionDeleteOperation<T> query(Query query);
    Query getQuery();
    int execute();
}
