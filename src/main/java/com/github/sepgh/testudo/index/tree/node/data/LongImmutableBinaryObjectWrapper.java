package com.github.sepgh.testudo.index.tree.node.data;

import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Longs;

public class LongImmutableBinaryObjectWrapper extends AbstractImmutableBinaryObjectWrapper<Long> {
    public static int BYTES = Long.BYTES + 1;

    public LongImmutableBinaryObjectWrapper() {
    }

    public LongImmutableBinaryObjectWrapper(byte[] bytes) {
        super(bytes);
    }

    @Override
    public LongImmutableBinaryObjectWrapper load(Long aLong) {
        byte[] bytes = new byte[this.size()];
        bytes[0] = 0x01;
        System.arraycopy(Longs.toByteArray(aLong), 0, bytes, 1, Long.BYTES);
        return new LongImmutableBinaryObjectWrapper(bytes);
    }

    @Override
    public LongImmutableBinaryObjectWrapper load(byte[] bytes, int beginning) {
        byte[] value = new byte[BYTES];
        System.arraycopy(
                bytes,
                beginning,
                value,
                0,
                this.size()
        );
        return new LongImmutableBinaryObjectWrapper(value);
    }

    @Override
    public Long asObject() {
        return BinaryUtils.bytesToLong(bytes, 1);
    }

    @Override
    public boolean hasValue() {
        return bytes[0] == 0x01;
    }

    @Override
    public int size() {
        return BYTES;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
