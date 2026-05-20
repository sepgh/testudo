package io.github.sepgh.testudo.core.view;

import io.github.sepgh.testudo.core.Page;

import java.lang.foreign.MemorySegment;

public interface View<T> extends Comparable<T> {
    <V extends View<T>> V pointTo(long offset);
    T get();
    void set(T value);
    int size();
}