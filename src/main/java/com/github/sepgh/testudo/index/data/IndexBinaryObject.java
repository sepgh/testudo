package com.github.sepgh.testudo.index.data;

public interface IndexBinaryObject<E> {
    E asObject();
    int size();
    byte[] getBytes();
}
