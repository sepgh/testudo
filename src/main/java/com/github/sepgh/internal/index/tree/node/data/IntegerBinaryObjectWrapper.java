package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import lombok.Getter;

@Getter
public class IntegerBinaryObjectWrapper implements BinaryObjectWrapper<Integer> {
    public static int BYTES = Integer.BYTES + 1;
    private byte[] bytes;
    @Override
    public IntegerBinaryObjectWrapper load(Integer integer) {
        this.bytes = new byte[this.size()];
        this.bytes[0] = 0x01;
        System.arraycopy(Ints.toByteArray(integer), 0, this.bytes, 1, Integer.BYTES);
        return this;
    }

    @Override
    public IntegerBinaryObjectWrapper load(byte[] bytes, int beginning) {
        this.bytes = new byte[this.size()];
        System.arraycopy(bytes, beginning, this.bytes, 0, this.size());
        return this;
    }

    @Override
    public Integer asObject() {
        return BinaryUtils.bytesToInteger(bytes, 1);
    }

    @Override
    public Class<Integer> getObjectClass() {
        return Integer.TYPE;
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
