package com.github.sepgh.testudo.index.tree.node.data;

import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;

@Getter
public class IntegerIndexBinaryObject extends AbstractIndexBinaryObject<Integer> {
    public static int BYTES = Integer.BYTES + 1;

    public IntegerIndexBinaryObject() {
    }

    protected IntegerIndexBinaryObject(byte[] bytes) {
        super(bytes);
    }

    @Override
    public Integer asObject() {
        return BinaryUtils.bytesToInteger(bytes, 1);
    }

    @Override
    public boolean hasValue() {
        return bytes[0] == 0x01;
    }

    @Override
    public int size() {
        return BYTES;
    }

    public static class Factory implements IndexBinaryObjectFactory<Integer> {
        @Override
        public IndexBinaryObject<Integer> create(Integer integer) {
            byte[] bytes = new byte[BYTES];
            bytes[0] = 0x01;
            System.arraycopy(Ints.toByteArray(integer), 0, bytes, 1, Integer.BYTES);
            return new IntegerIndexBinaryObject(bytes);
        }

        @Override
        public IndexBinaryObject<Integer> create(byte[] bytes, int beginning) {
            byte[] value = new byte[BYTES];
            System.arraycopy(bytes, beginning, value, 0, BYTES);
            return new IntegerIndexBinaryObject(value);
        }

        @Override
        public int size() {
            return BYTES;
        }
    }
}
