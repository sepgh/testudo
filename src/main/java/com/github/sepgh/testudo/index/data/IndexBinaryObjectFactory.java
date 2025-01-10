package com.github.sepgh.testudo.index.data;

import com.github.sepgh.testudo.exception.IndexBinaryObjectCreationException;

public interface IndexBinaryObjectFactory<E> {
    IndexBinaryObject<E> create(E e) throws IndexBinaryObjectCreationException;
    IndexBinaryObject<E> create(byte[] bytes, int beginning);
    default IndexBinaryObject<E> create(byte[] bytes) {
        return create(bytes, 0);
    }
    int size();
    Class<E> getType();
    IndexBinaryObject<E> createEmpty();
}
