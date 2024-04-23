package com.github.sepgh.internal.utils;

public class BinaryUtils {
    public static long bytesToLong(final byte[] b, int originIndex) {
        long result = 0;
        for (int i = originIndex; i < originIndex + Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    public static int bytesToInteger(final byte[] b, int originIndex) {
        int result = 0;
        for (int i = originIndex; i < originIndex + Integer.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        return result;
    }
}
