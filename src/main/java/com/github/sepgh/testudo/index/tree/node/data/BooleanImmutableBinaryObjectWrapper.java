package com.github.sepgh.testudo.index.tree.node.data;

import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;

@Getter
public class BooleanImmutableBinaryObjectWrapper extends AbstractImmutableBinaryObjectWrapper<Boolean> {
    public static int BYTES = 1;

    public BooleanImmutableBinaryObjectWrapper() {
    }

    protected BooleanImmutableBinaryObjectWrapper(byte[] bytes) {
        super(bytes);
    }

    @Override
    public BooleanImmutableBinaryObjectWrapper load(Boolean val) {
        byte[] bytes = new byte[this.size()];
        bytes[0] = 0x01;
        System.arraycopy(new byte[]{(byte) (val ? 1 : 0)}, 0, bytes, 1, BYTES);
        return new BooleanImmutableBinaryObjectWrapper(bytes);
    }

    @Override
    public BooleanImmutableBinaryObjectWrapper load(byte[] bytes, int beginning) {
        byte[] value = new byte[this.size()];
        System.arraycopy(bytes, beginning, value, 0, this.size());
        return new BooleanImmutableBinaryObjectWrapper(value);
    }

    @Override
    public Boolean asObject() {
        return bytes[0] == 0x01;
    }

    @Override
    public boolean hasValue() {
        return true;
    }

    @Override
    public int size() {
        return BYTES;
    }
}
