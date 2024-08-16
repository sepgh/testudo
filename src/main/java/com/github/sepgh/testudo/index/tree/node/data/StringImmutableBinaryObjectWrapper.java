package com.github.sepgh.testudo.index.tree.node.data;

import java.nio.charset.StandardCharsets;

public class StringImmutableBinaryObjectWrapper extends AbstractImmutableBinaryObjectWrapper<String> {
    public final int BYTES;
    private byte[] bytes;

    public StringImmutableBinaryObjectWrapper(int bytesCount) {
        this.BYTES = bytesCount;
    }

    public StringImmutableBinaryObjectWrapper(byte[] bytes) {
        this.bytes = bytes;
        this.BYTES = bytes.length;
    }

    @Override
    public ImmutableBinaryObjectWrapper<String> load(String s) throws InvalidBinaryObjectWrapperValue {
        byte[] temp = s.getBytes(StandardCharsets.UTF_8);
        if (temp.length > BYTES) {
            throw new InvalidBinaryObjectWrapperValue(s, this.getClass());
        }

        byte[] result = new byte[BYTES];

        System.arraycopy(temp, 0, result, 0, temp.length);

        for (int i = temp.length; i < BYTES; i++) {
            result[i] = 0;
        }

        return new StringImmutableBinaryObjectWrapper(result);
    }

    @Override
    public ImmutableBinaryObjectWrapper<String> load(byte[] bytes, int beginning) {
        this.bytes = new byte[BYTES];
        System.arraycopy(
                bytes,
                beginning,
                this.bytes,
                0,
                this.size()
        );
        return this;
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
}
