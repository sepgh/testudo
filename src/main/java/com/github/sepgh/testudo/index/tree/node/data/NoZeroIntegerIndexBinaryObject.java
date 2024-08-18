package com.github.sepgh.testudo.index.tree.node.data;

import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;

@Getter
public class NoZeroIntegerIndexBinaryObject extends AbstractIndexBinaryObject<Integer> {
    public static int BYTES = Integer.BYTES;

    public NoZeroIntegerIndexBinaryObject() {
    }

    public NoZeroIntegerIndexBinaryObject(byte[] bytes) {
        super(bytes);
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

    public static class Factory implements IndexBinaryObjectFactory<Integer> {

        @Override
        public IndexBinaryObject<Integer> create(Integer integer) throws InvalidIndexBinaryObject {
            if (integer == 0)
                throw new InvalidIndexBinaryObject(integer, NoZeroIntegerIndexBinaryObject.class);
            return new NoZeroIntegerIndexBinaryObject(Ints.toByteArray(integer));
        }

        @Override
        public IndexBinaryObject<Integer> create(byte[] bytes, int beginning) {
            byte[] value = new byte[BYTES];
            System.arraycopy(bytes, beginning, value, 0, Integer.BYTES);
            return new NoZeroIntegerIndexBinaryObject(value);
        }

        @Override
        public int size() {
            return BYTES;
        }
    }
}
