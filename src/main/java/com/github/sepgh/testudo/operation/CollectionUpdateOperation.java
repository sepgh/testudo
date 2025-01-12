package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.BaseSerializationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.operation.query.Query;

import java.util.function.Consumer;

public interface CollectionUpdateOperation<T extends Number & Comparable<T>> {
    CollectionUpdateOperation<T> query(Query query);
    Query getQuery();
    <M> long execute(Class<M> mClass, Consumer<M> mConsumer) throws InternalOperationException, BaseSerializationException;
    long execute(Consumer<byte[]> byteArrayConsumer) throws InternalOperationException, BaseSerializationException;
}
