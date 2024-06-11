package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Longs;
import lombok.Getter;

@Getter
public class NoZeroLongBinaryObjectWrapper implements BinaryObjectWrapper<Long> {
    public static int BYTES = Long.BYTES;
    private byte[] bytes;
    @Override
    public NoZeroLongBinaryObjectWrapper load(Long aLong) throws InvalidBinaryObjectWrapperValue {
        if (aLong == 0)
            throw new InvalidBinaryObjectWrapperValue(aLong, this.getClass());
        this.bytes = Longs.toByteArray(aLong);
        return this;
    }

    @Override
    public NoZeroLongBinaryObjectWrapper load(byte[] bytes, int beginning) {
        this.bytes = new byte[BYTES];
        System.arraycopy(bytes, beginning, this.bytes, 0, Long.BYTES);
        return this;
    }

    @Override
    public Long asObject() {
        return BinaryUtils.bytesToLong(bytes, 0);
    }

    @Override
    public Class<Long> getObjectClass() {
        return Long.TYPE;
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
