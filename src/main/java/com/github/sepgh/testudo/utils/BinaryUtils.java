package com.github.sepgh.testudo.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;

import java.util.Arrays;

public class BinaryUtils {
    public static long bytesToLong(final byte[] b, int originIndex) {
        return Longs.fromByteArray(Arrays.copyOfRange(b, originIndex, originIndex + Long.BYTES));
    }

    public static int bytesToInteger(final byte[] b, int originIndex) {
        return Ints.fromByteArray(Arrays.copyOfRange(b, originIndex, originIndex + Integer.BYTES));
    }

    public static byte[] toByteArray(UnsignedInteger unsignedInteger) {
        byte[] byteArray = unsignedInteger.bigIntegerValue().toByteArray();

        // Ensure the array is exactly 4 bytes long
        if (byteArray.length == 4) {
            return byteArray;
        } else if (byteArray.length < 4) {
            // If the byte array is less than 4 bytes, pad with leading zeros
            byte[] paddedArray = new byte[4];
            System.arraycopy(byteArray, 0, paddedArray, 4 - byteArray.length, byteArray.length);
            return paddedArray;
        } else {
            // If the byte array is more than 4 bytes, truncate it (remove leading sign byte)
            return Arrays.copyOfRange(byteArray, byteArray.length - 4, byteArray.length);
        }
    }

    public static byte[] toByteArray(UnsignedLong unsignedLong) {
        byte[] byteArray = unsignedLong.bigIntegerValue().toByteArray();

        // Ensure the array is exactly 8 bytes long
        if (byteArray.length == 8) {
            return byteArray;
        } else if (byteArray.length < 8) {
            // If the byte array is less than 8 bytes, pad with leading zeros
            byte[] paddedArray = new byte[8];
            System.arraycopy(byteArray, 0, paddedArray, 8 - byteArray.length, byteArray.length);
            return paddedArray;
        } else {
            // If the byte array is more than 8 bytes, truncate it (remove leading sign byte)
            return Arrays.copyOfRange(byteArray, byteArray.length - 8, byteArray.length);
        }
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
