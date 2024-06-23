package com.github.sepgh.testudo.index.tree.node.data;

import lombok.Getter;

@Getter
public abstract class AbstractImmutableBinaryObjectWrapper<E extends Comparable<E>> implements ImmutableBinaryObjectWrapper<E> {
    protected byte[] bytes;

    public AbstractImmutableBinaryObjectWrapper() {
    }

    protected AbstractImmutableBinaryObjectWrapper(byte[] bytes) {
        this.bytes = bytes;
    }
}
