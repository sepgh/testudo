package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.index.DuplicateQueryableIndex;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.query.Queryable;
import com.github.sepgh.testudo.scheme.Scheme;


public interface CollectionIndexProvider {
    UniqueQueryableIndex<?, ? extends Number> getUniqueIndexManager(Scheme.Field field);
    DuplicateQueryableIndex<?, ? extends Number> getDuplicateIndexManager(Scheme.Field field);
    UniqueTreeIndexManager<?, Pointer> getClusterIndexManager();
    UniqueQueryableIndex<?, ? extends Number> getUniqueIndexManager(String fieldName);
    DuplicateQueryableIndex<?, ? extends Number> getDuplicateIndexManager(String fieldName);
    Queryable<?, ? extends Number> getQueryableIndex(Scheme.Field field);
    Queryable<?, ? extends Number> getQueryableIndex(String fieldName);
}
