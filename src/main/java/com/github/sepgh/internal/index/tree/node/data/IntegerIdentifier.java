package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Ints;

public class IntegerIdentifier extends NodeInnerObj<Integer> {
    public static final int BYTES = Integer.BYTES + 1;

    public IntegerIdentifier(byte[] bytes, int beginning) {
        super(bytes, beginning);
    }

    public IntegerIdentifier(byte[] bytes) {
        super(bytes);
    }

    public IntegerIdentifier(Integer integer) {
        super(integer);
    }

    @Override
    protected byte[] valueToByteArray(Integer integer) {
        byte[] result = new byte[BYTES];
        result[0] = 0x01;
        System.arraycopy(
                Ints.toByteArray(integer),
                0,
                result,
                1,
                Integer.BYTES
        );

        return result;
    }

    @Override
    public boolean exists() {
        return bytes[0] == 0x01;
    }

    @Override
    public Integer data() {
        return BinaryUtils.bytesToInteger(bytes, 1);
    }

    @Override
    public int size() {
        return BYTES;
    }

}
