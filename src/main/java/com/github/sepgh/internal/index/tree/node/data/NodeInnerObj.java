package com.github.sepgh.internal.index.tree.node.data;

import lombok.Getter;

@Getter
public abstract class NodeInnerObj<V extends Comparable<V>> {
    protected byte[] bytes;
    protected int beginning;

    public NodeInnerObj(byte[] bytes, int beginning) {
        this.bytes = new byte[this.size()];
        System.arraycopy(
                bytes,
                beginning,
                this.bytes,
                0,
                this.size()
        );
    }

    public NodeInnerObj(byte[] bytes) {
        this(bytes, 0);
    }

    public NodeInnerObj(V v){
        this.bytes = this.valueToByteArray(v);
        this.beginning = 0;
    }

    protected abstract byte[] valueToByteArray(V v);

    public abstract boolean exists();
    public abstract V data();
    public abstract int size();
}
