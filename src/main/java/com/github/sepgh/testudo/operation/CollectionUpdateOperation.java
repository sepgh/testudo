package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.operation.query.Query;

import java.util.function.Consumer;

public interface CollectionUpdateOperation<T extends Number & Comparable<T>> {
    CollectionUpdateOperation<T> query(Query query);
    Query getQuery();
    <M> int execute(Consumer<M> mConsumer, Class<M> mClass);
    int execute(Consumer<byte[]> byteArrayConsumer);
}
