package com.github.sepgh.testudo.index.tree.node.data;

import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Longs;
import lombok.Getter;

@Getter
public class NoZeroLongImmutableBinaryObjectWrapper extends AbstractImmutableBinaryObjectWrapper<Long> {
    public static int BYTES = Long.BYTES;

    public NoZeroLongImmutableBinaryObjectWrapper() {
    }

    public NoZeroLongImmutableBinaryObjectWrapper(byte[] bytes) {
        super(bytes);
    }

    @Override
    public NoZeroLongImmutableBinaryObjectWrapper load(Long aLong) throws InvalidBinaryObjectWrapperValue {
        if (aLong == 0)
            throw new InvalidBinaryObjectWrapperValue(aLong, this.getClass());
        return new NoZeroLongImmutableBinaryObjectWrapper(Longs.toByteArray(aLong));
    }

    @Override
    public NoZeroLongImmutableBinaryObjectWrapper load(byte[] bytes, int beginning) {
        byte[] value = new byte[BYTES];
        System.arraycopy(bytes, beginning, value, 0, Long.BYTES);
        return new NoZeroLongImmutableBinaryObjectWrapper(value);
    }

    @Override
    public Long asObject() {
        return BinaryUtils.bytesToLong(bytes, 0);
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
