package com.github.sepgh.testudo.context;

import com.github.sepgh.testudo.operation.CollectionOperation;
import com.github.sepgh.testudo.scheme.Scheme;

public interface DatabaseContext {
    CollectionOperation getOperation(Scheme.Collection collection);
    CollectionOperation getOperation(String collection);
}