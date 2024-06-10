package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Ints;

public class NoZeroIntegerIdentifier extends NodeData<Integer> {
    public static final int BYTES = Integer.BYTES;

    public NoZeroIntegerIdentifier(byte[] bytes, int beginning) {
        super(bytes, beginning);
    }

    public NoZeroIntegerIdentifier(byte[] bytes) {
        super(bytes);
    }

    public NoZeroIntegerIdentifier(Integer integer) {
        super(integer);
    }

    @Override
    protected byte[] valueToByteArray(Integer integer) {
        return Ints.toByteArray(integer);
    }

    @Override
    public boolean exists() {
        return data() != 0;
    }

    @Override
    public Integer data() {
        return BinaryUtils.bytesToInteger(bytes, 0);
    }

    @Override
    public int size() {
        return BYTES;
    }

}
