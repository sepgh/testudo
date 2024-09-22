package com.github.sepgh.testudo.utils;

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

    public static boolean isAllZeros(byte[] array, int offset, int size) {
        int end = offset + size;
        for (int i = offset; i < end; i++) {
            if (array[i] != 0) {
                return false;  // Short-circuit as soon as a non-zero byte is found
            }
        }
        return true;  // All bytes in the range are zero
    }
}
