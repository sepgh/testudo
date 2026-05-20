package io.github.sepgh.testudo.core.view;

public interface View<T> extends Comparable<T> {
    <V extends View<T>> V pointTo(long offset);
    T get();
    void set(T value);
    int size();
}