package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Longs;
import lombok.Getter;

@Getter
public class LongBinaryObjectWrapper implements BinaryObjectWrapper<Long> {
    public static int BYTES = Long.BYTES + 1;
    private byte[] bytes;
    @Override
    public LongBinaryObjectWrapper load(Long aLong) {
        this.bytes = new byte[BYTES];
        this.bytes[0] = 0x01;
        System.arraycopy(Longs.toByteArray(aLong), 0, this.bytes, 1, Long.BYTES);
        return this;
    }

    @Override
    public LongBinaryObjectWrapper load(byte[] bytes, int beginning) {
        this.bytes = new byte[BYTES];
        this.bytes[0] = bytes[0];
        System.arraycopy(bytes, beginning, this.bytes, 1, Long.BYTES);
        return this;
    }

    @Override
    public Long asObject() {
        return BinaryUtils.bytesToLong(bytes, 1);
    }

    @Override
    public Class<Long> getObjectClass() {
        return Long.TYPE;
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
