package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.index.Pointer;

public class PointerInnerObject extends NodeInnerObj<Pointer> {
    public static final int BYTES = Pointer.BYTES;

    public PointerInnerObject(byte[] bytes, int beginning) {
        super(bytes, beginning);
    }

    public PointerInnerObject(byte[] bytes) {
        this(bytes, 0);
    }

    public PointerInnerObject(Pointer pointer) {
        super(pointer);
    }

    @Override
    protected byte[] valueToByteArray(Pointer pointer) {
        return pointer.toBytes();
    }

    @Override
    public boolean exists() {
        return bytes[0] == 0x01;
    }

    @Override
    public Pointer data() {
        return Pointer.fromBytes(bytes, beginning);
    }

    @Override
    public int size() {
        return BYTES;
    }

}
