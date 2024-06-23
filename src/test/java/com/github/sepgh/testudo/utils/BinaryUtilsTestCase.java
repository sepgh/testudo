package com.github.sepgh.testudo.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BinaryUtilsTestCase {

    @Test
    public void testBytesToInteger(){
        byte[] oneByteRepresentation = new byte[]{(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x01};
        Assertions.assertEquals(
                1,
                BinaryUtils.bytesToInteger(oneByteRepresentation, 0),
                "The representation of int 1 and the 'binary to int' don't match.");
    }

    @Test
    public void testBytesToIntegerFromPosition(){
        byte[] oneByteRepresentation = new byte[]{(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x01};
        Assertions.assertEquals(
                1,
                BinaryUtils.bytesToInteger(oneByteRepresentation, 1),
                "The representation of int 1 and the 'binary to int' at position 1 don't match.");
    }

    @Test
    public void testBytesToLong(){
        byte[] oneByteRepresentation = new byte[]{
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x01
        };
        Assertions.assertEquals(
                1,
                BinaryUtils.bytesToLong(oneByteRepresentation, 0),
                "The representation of int 1 and the 'binary to int' don't match.");
    }

    @Test
    public void testBytesToLongFromPosition(){
        byte[] oneByteRepresentation = new byte[]{
                (byte)0x00,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x01
        };
        Assertions.assertEquals(
                1,
                BinaryUtils.bytesToLong(oneByteRepresentation, 1),
                "The representation of int 1 and the 'binary to int' at position 1 don't match.");
    }


}
