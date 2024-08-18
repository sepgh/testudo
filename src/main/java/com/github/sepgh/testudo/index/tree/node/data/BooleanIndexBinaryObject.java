package com.github.sepgh.testudo.index.tree.node.data;

import lombok.Getter;

@Getter
public class BooleanIndexBinaryObject extends AbstractIndexBinaryObject<Boolean> {
    public static int BYTES = 1;

    public BooleanIndexBinaryObject() {
    }

    protected BooleanIndexBinaryObject(byte[] bytes) {
        super(bytes);
    }

    @Override
    public Boolean asObject() {
        return bytes[0] == 0x01;
    }

    @Override
    public boolean hasValue() {
        return true;
    }

    @Override
    public int size() {
        return BYTES;
    }

    public static class Factory implements IndexBinaryObjectFactory<Boolean> {
        @Override
        public IndexBinaryObject<Boolean> create(Boolean val) {
            byte[] bytes = new byte[BYTES];
            bytes[0] = 0x01;
            System.arraycopy(new byte[]{(byte) (val ? 1 : 0)}, 0, bytes, 1, BYTES);
            return new BooleanIndexBinaryObject(bytes);
        }

        @Override
        public IndexBinaryObject<Boolean> create(byte[] bytes, int beginning) {
            byte[] value = new byte[BYTES];
            System.arraycopy(bytes, beginning, value, 0, BYTES);
            return new BooleanIndexBinaryObject(value);
        }

        @Override
        public int size() {
            return BYTES;
        }
    }
}
