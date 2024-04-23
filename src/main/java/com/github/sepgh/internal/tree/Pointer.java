package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.utils.BinaryUtils;

/*
   A pointer may point to where data begins (on table) or to another node
   Binary size of pointer:  1byte type  +  8bytes position   +  4bytes chunk  = 13bytes
*/
public record Pointer(byte type, long position, int chunk) {
    public static byte TYPE_DATA = 0x01;
    public static byte TYPE_NODE = 0x02;
    public static int POINTER_SIZE = 13;

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

    public boolean isDataPointer(){
        return type == TYPE_DATA;
    }

    public boolean isNodePointer(){
        return type == TYPE_NODE;
    }

}
