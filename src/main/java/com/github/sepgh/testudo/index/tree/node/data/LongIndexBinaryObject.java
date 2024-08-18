package com.github.sepgh.testudo.index.tree.node.data;

import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Longs;

public class LongIndexBinaryObject extends AbstractIndexBinaryObject<Long> {
    public static int BYTES = Long.BYTES + 1;

    public LongIndexBinaryObject() {
    }

    public LongIndexBinaryObject(byte[] bytes) {
        super(bytes);
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

    public static class Factory implements IndexBinaryObjectFactory<Long> {

        @Override
        public IndexBinaryObject<Long> create(Long aLong) {
            byte[] bytes = new byte[BYTES];
            bytes[0] = 0x01;
            System.arraycopy(Longs.toByteArray(aLong), 0, bytes, 1, Long.BYTES);
            return new LongIndexBinaryObject(bytes);
        }

        @Override
        public IndexBinaryObject<Long> create(byte[] bytes, int beginning) {
            byte[] value = new byte[BYTES];
            System.arraycopy(
                    bytes,
                    beginning,
                    value,
                    0,
                    BYTES
            );
            return new LongIndexBinaryObject(value);
        }

        @Override
        public int size() {
            return BYTES;
        }
    }
}
