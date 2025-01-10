package com.github.sepgh.testudo.functional;


import com.github.sepgh.testudo.exception.InvalidDBObjectWrapper;

@FunctionalInterface
public interface DBObjectUpdateConsumer<T> {
    void accept(T t) throws InvalidDBObjectWrapper;
}
