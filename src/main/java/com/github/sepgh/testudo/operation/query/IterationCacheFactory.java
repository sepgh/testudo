package com.github.sepgh.testudo.operation.query;

public interface IterationCacheFactory {
    <T extends Number> IterationCache<T> create();
}
