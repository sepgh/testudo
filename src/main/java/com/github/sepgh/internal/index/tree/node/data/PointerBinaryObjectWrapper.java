package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.index.Pointer;
import lombok.Getter;

@Getter
public class PointerBinaryObjectWrapper implements BinaryObjectWrapper<Pointer> {
    public static int BYTES = Pointer.BYTES;
    private byte[] bytes;
    @Override
    public PointerBinaryObjectWrapper load(Pointer pointer) {
        this.bytes = pointer.toBytes();
        return this;
    }

    @Override
    public PointerBinaryObjectWrapper load(byte[] bytes, int beginning) {
        this.bytes = new byte[BYTES];
        System.arraycopy(bytes, beginning, this.bytes, 0, Pointer.BYTES);
        return this;
    }

    @Override
    public Pointer asObject() {
        return Pointer.fromBytes(bytes);
    }

    @Override
    public Class<Pointer> getObjectClass() {
        return Pointer.class;
    }

    @Override
    public boolean hasValue() {
        return bytes[0] != 0x00;
    }

    @Override
    public int size() {
        return BYTES;
    }
}
