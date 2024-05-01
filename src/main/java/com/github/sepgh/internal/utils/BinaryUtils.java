package com.github.sepgh.internal.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.util.Arrays;

public class BinaryUtils {
    public static long bytesToLong(final byte[] b, int originIndex) {
        return Longs.fromByteArray(Arrays.copyOfRange(b, originIndex, originIndex + Long.BYTES));
    }

    public static int bytesToInteger(final byte[] b, int originIndex) {
        return Ints.fromByteArray(Arrays.copyOfRange(b, originIndex, originIndex + Integer.BYTES));
    }
}
