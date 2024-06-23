package com.github.sepgh.testudo.index.tree.node.data;

import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;

@Getter
public class NoZeroIntegerImmutableBinaryObjectWrapper extends AbstractImmutableBinaryObjectWrapper<Integer> {
    public static int BYTES = Integer.BYTES;

    public NoZeroIntegerImmutableBinaryObjectWrapper() {
    }

    public NoZeroIntegerImmutableBinaryObjectWrapper(byte[] bytes) {
        super(bytes);
    }

    @Override
    public NoZeroIntegerImmutableBinaryObjectWrapper load(Integer integer) throws InvalidBinaryObjectWrapperValue {
        if (integer == 0)
            throw new InvalidBinaryObjectWrapperValue(integer, this.getClass());
        return new NoZeroIntegerImmutableBinaryObjectWrapper(Ints.toByteArray(integer));
    }

    @Override
    public NoZeroIntegerImmutableBinaryObjectWrapper load(byte[] bytes, int beginning) {
        byte[] value = new byte[BYTES];
        System.arraycopy(bytes, beginning, value, 0, Integer.BYTES);
        return new NoZeroIntegerImmutableBinaryObjectWrapper(value);
    }

    @Override
    public Integer asObject() {
        return BinaryUtils.bytesToInteger(bytes, 0);
    }

    @Override
    public boolean hasValue() {
        return asObject() != 0;
    }

    @Override
    public int size() {
        return BYTES;
    }
}
