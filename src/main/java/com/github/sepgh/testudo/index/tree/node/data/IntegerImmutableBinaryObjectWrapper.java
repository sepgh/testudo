package com.github.sepgh.testudo.index.tree.node.data;

import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;

@Getter
public class IntegerImmutableBinaryObjectWrapper extends AbstractImmutableBinaryObjectWrapper<Integer> {
    public static int BYTES = Integer.BYTES + 1;

    public IntegerImmutableBinaryObjectWrapper() {
    }

    protected IntegerImmutableBinaryObjectWrapper(byte[] bytes) {
        super(bytes);
    }

    @Override
    public IntegerImmutableBinaryObjectWrapper load(Integer integer) {
        byte[] bytes = new byte[this.size()];
        bytes[0] = 0x01;
        System.arraycopy(Ints.toByteArray(integer), 0, bytes, 1, Integer.BYTES);
        return new IntegerImmutableBinaryObjectWrapper(bytes);
    }

    @Override
    public IntegerImmutableBinaryObjectWrapper load(byte[] bytes, int beginning) {
        byte[] value = new byte[this.size()];
        System.arraycopy(bytes, beginning, value, 0, this.size());
        return new IntegerImmutableBinaryObjectWrapper(value);
    }

    @Override
    public Integer asObject() {
        return BinaryUtils.bytesToInteger(bytes, 1);
    }

    @Override
    public boolean hasValue() {
        return bytes[0] == 0x01;
    }

    @Override
    public int size() {
        return BYTES;
    }
}
