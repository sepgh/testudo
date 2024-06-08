package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Longs;

public class NoZeroLongIdentifier extends NodeInnerObj<Long> {
    public static final int BYTES = Long.BYTES;

    public NoZeroLongIdentifier(byte[] bytes, int beginning) {
        super(bytes, beginning);
    }

    public NoZeroLongIdentifier(byte[] bytes) {
        super(bytes);
    }

    public NoZeroLongIdentifier(Long aLong) {
        super(aLong);
    }

    @Override
    protected byte[] valueToByteArray(Long aLong) {
        return Longs.toByteArray(aLong);
    }

    @Override
    public boolean exists() {
        return data() != 0;
    }

    @Override
    public Long data() {
        return BinaryUtils.bytesToLong(bytes, 0);
    }

    @Override
    public int size() {
        return BYTES;
    }

}
