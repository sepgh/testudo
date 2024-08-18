package com.github.sepgh.testudo.index.tree.node.data;

public interface IndexBinaryObjectFactory<E extends Comparable<E>> {
    IndexBinaryObject<E> create(E e) throws IndexBinaryObject.InvalidIndexBinaryObject;
    IndexBinaryObject<E> create(byte[] bytes, int beginning);
    default IndexBinaryObject<E> create(byte[] bytes) {
        return create(bytes, 0);
    }
    int size();
}
