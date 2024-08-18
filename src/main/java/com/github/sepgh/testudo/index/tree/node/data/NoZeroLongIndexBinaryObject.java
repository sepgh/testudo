package com.github.sepgh.testudo.index.tree.node.data;

import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Longs;
import lombok.Getter;

@Getter
public class NoZeroLongIndexBinaryObject extends AbstractIndexBinaryObject<Long> {
    public static int BYTES = Long.BYTES;

    public NoZeroLongIndexBinaryObject() {
    }

    public NoZeroLongIndexBinaryObject(byte[] bytes) {
        super(bytes);
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

    public static class Factory implements IndexBinaryObjectFactory<Long> {

        @Override
        public IndexBinaryObject<Long> create(Long aLong) throws InvalidIndexBinaryObject {
            if (aLong == 0)
                throw new InvalidIndexBinaryObject(aLong, LongIndexBinaryObject.class);
            return new NoZeroLongIndexBinaryObject(Longs.toByteArray(aLong));
        }

        @Override
        public IndexBinaryObject<Long> create(byte[] bytes, int beginning) {
            byte[] value = new byte[BYTES];
            System.arraycopy(bytes, beginning, value, 0, Long.BYTES);
            return new NoZeroLongIndexBinaryObject(value);
        }

        @Override
        public int size() {
            return BYTES;
        }
    }
}
