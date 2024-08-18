package com.github.sepgh.testudo.index.tree.node.data;

import java.nio.charset.StandardCharsets;

public class StringIndexBinaryObject extends AbstractIndexBinaryObject<String> {
    public final int BYTES;
    private byte[] bytes;

    public StringIndexBinaryObject(int bytesCount) {
        this.BYTES = bytesCount;
    }

    public StringIndexBinaryObject(byte[] bytes) {
        this.bytes = bytes;
        this.BYTES = bytes.length;
    }

    @Override
    public String asObject() {
        int len = 0;
        while (len < bytes.length && bytes[len] != 0) {
            len++;
        }
        return new String(bytes, 0, len, StandardCharsets.UTF_8);
    }

    @Override
    public boolean hasValue() {
        for (byte aByte : this.bytes) {
            if (aByte != 0x00)
                return true;
        }
        return false;
    }

    @Override
    public int size() {
        return BYTES;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

    public static class Factory implements IndexBinaryObjectFactory<String> {
        private final int size;

        public Factory(int size) {
            this.size = size;
        }

        @Override
        public IndexBinaryObject<String> create(String s) throws InvalidIndexBinaryObject {
            byte[] temp = s.getBytes(StandardCharsets.UTF_8);
            if (temp.length > size) {
                throw new InvalidIndexBinaryObject(s, StringIndexBinaryObject.class);
            }

            byte[] result = new byte[size];

            System.arraycopy(temp, 0, result, 0, temp.length);

            for (int i = temp.length; i < size; i++) {
                result[i] = 0;
            }

            return new StringIndexBinaryObject(result);
        }

        @Override
        public IndexBinaryObject<String> create(byte[] bytes, int beginning) {
            byte[] data = new byte[size];
            System.arraycopy(
                    bytes,
                    beginning,
                    data,
                    0,
                    this.size()
            );
            return new StringIndexBinaryObject(data);
        }

        @Override
        public int size() {
            return size;
        }
    }
}
