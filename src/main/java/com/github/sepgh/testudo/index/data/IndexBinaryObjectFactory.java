package com.github.sepgh.testudo.index.data;

public interface IndexBinaryObjectFactory<E> {
    IndexBinaryObject<E> create(E e);
    IndexBinaryObject<E> create(byte[] bytes, int beginning);
    default IndexBinaryObject<E> create(byte[] bytes) {
        return create(bytes, 0);
    }
    int size();
    Class<E> getType();
}
