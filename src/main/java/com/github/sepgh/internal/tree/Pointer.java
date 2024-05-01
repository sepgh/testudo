package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.ToString;

/*
   A pointer may point to where data begins (on table) or to another node
   Binary size of pointer:  1byte type  +  8bytes position   +  4bytes chunk  = 13bytes
*/

public record Pointer(byte type, long position, int chunk) {
    public static byte TYPE_DATA = 0x01;
    public static byte TYPE_NODE = 0x02;
    public static int BYTES = 1 + Long.BYTES + Integer.BYTES;

    public static Pointer fromByteArray(byte[] bytes, int position){
        return new Pointer(
                bytes[position],
                BinaryUtils.bytesToLong(bytes, position + 1),
                BinaryUtils.bytesToInteger(bytes, position + 1 + Long.BYTES)
        );
    }

    public static Pointer fromByteArray(byte[] bytes){
        return Pointer.fromByteArray(bytes, 0);
    }

    public byte[] toByteArray() {
        byte[] bytes = new byte[BYTES];
        bytes[0] = type;
        System.arraycopy(Longs.toByteArray(position), 0, bytes, 1, Long.BYTES);
        System.arraycopy(Ints.toByteArray(chunk), 0, bytes, 1 + Long.BYTES, Integer.BYTES);
        return bytes;
    }

    // A quick way to write the current pointer into a specific position of a byte array (which is node itself)
    public void fillByteArrayWithPointer(byte[] source, int position) {
        source[position] = type;
        System.arraycopy(Longs.toByteArray(this.position), 0, source, position + 1, Long.BYTES);
        System.arraycopy(Ints.toByteArray(chunk), 0, source, position + 1 + Long.BYTES, Integer.BYTES);
    }

    public boolean isDataPointer(){
        return type == TYPE_DATA;
    }

    public boolean isNodePointer(){
        return type == TYPE_NODE;
    }

    @Override
    public String toString() {
        return "Pointer{" +
                "type=" + type +
                ", position=" + position +
                ", chunk=" + chunk +
                '}';
    }
}
