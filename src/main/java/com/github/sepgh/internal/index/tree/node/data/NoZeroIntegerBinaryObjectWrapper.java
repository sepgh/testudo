package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;

@Getter
public class NoZeroIntegerBinaryObjectWrapper implements BinaryObjectWrapper<Integer> {
    public static int BYTES = Integer.BYTES;
    private byte[] bytes;
    @Override
    public NoZeroIntegerBinaryObjectWrapper load(Integer integer) throws InvalidBinaryObjectWrapperValue {
        if (integer == 0)
            throw new InvalidBinaryObjectWrapperValue(integer, this.getClass());
        this.bytes = Ints.toByteArray(integer);
        return this;
    }

    @Override
    public NoZeroIntegerBinaryObjectWrapper load(byte[] bytes, int beginning) {
        this.bytes = new byte[BYTES];
        System.arraycopy(bytes, beginning, this.bytes, 0, Integer.BYTES);
        return this;
    }

    @Override
    public Integer asObject() {
        return BinaryUtils.bytesToInteger(bytes, 0);
    }

    @Override
    public Class<Integer> getObjectClass() {
        return Integer.TYPE;
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
