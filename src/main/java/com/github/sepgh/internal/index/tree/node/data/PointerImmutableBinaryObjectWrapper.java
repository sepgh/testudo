package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.index.Pointer;
import lombok.Getter;

@Getter
public class PointerImmutableBinaryObjectWrapper extends AbstractImmutableBinaryObjectWrapper<Pointer> {
    public static int BYTES = Pointer.BYTES;

    public PointerImmutableBinaryObjectWrapper() {
    }

    public PointerImmutableBinaryObjectWrapper(byte[] bytes) {
        super(bytes);
    }

    @Override
    public PointerImmutableBinaryObjectWrapper load(Pointer pointer) {
        return new PointerImmutableBinaryObjectWrapper(pointer.toBytes());
    }

    @Override
    public PointerImmutableBinaryObjectWrapper load(byte[] bytes, int beginning) {
        byte[] value = new byte[BYTES];
        System.arraycopy(bytes, beginning, value, 0, Pointer.BYTES);
        return new PointerImmutableBinaryObjectWrapper(value);
    }

    @Override
    public Pointer asObject() {
        return Pointer.fromBytes(bytes);
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
