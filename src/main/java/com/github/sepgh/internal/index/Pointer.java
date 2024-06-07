package com.github.sepgh.internal.index;

import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.Data;

import java.util.Objects;

/*
   A pointer may point to where data begins (on table) or to another node
   Binary size of pointer:  1byte type  +  8bytes position   +  4bytes chunk  = 13bytes
*/

//@AllArgsConstructor
@Data
public class Pointer implements Comparable<Pointer> {
    public static byte TYPE_DATA = 0x01;
    public static byte TYPE_NODE = 0x02;
    public static int BYTES = 1 + Long.BYTES + Integer.BYTES;

    private final byte type;
    private long position;
    private int chunk;

    public Pointer(byte type, long position, int chunk) {
        this.type = type;
        this.position = position;
        this.chunk = chunk;
//        if (position < 0){
//            throw new RuntimeException("F");
//        }
    }

    public static Pointer fromBytes(byte[] bytes, int position){
//        System.out.println(HashCode.fromBytes(bytes));
        return new Pointer(
                bytes[position],
                BinaryUtils.bytesToLong(bytes, position + 1),
                BinaryUtils.bytesToInteger(bytes, position + 1 + Long.BYTES)
        );
    }

    public static Pointer empty(){
        return new Pointer(
                (byte) 0x00,
                0,
                0
        );
    }

    public static Pointer fromBytes(byte[] bytes){
        return Pointer.fromBytes(bytes, 0);
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[BYTES];
        bytes[0] = type;
        System.arraycopy(Longs.toByteArray(position), 0, bytes, 1, Long.BYTES);
        System.arraycopy(Ints.toByteArray(chunk), 0, bytes, 1 + Long.BYTES, Integer.BYTES);
        return bytes;
    }

    // A quick way to write the current pointer into a specific position of a byte array (which is node itself)
    public void fillBytes(byte[] source, int position) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pointer pointer = (Pointer) o;
        return type == pointer.type && position == pointer.position && chunk == pointer.chunk;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, position, chunk);
    }


    // This doesn't make much sense if single file for multiple table indexes is used
    @Override
    public int compareTo(Pointer other) {
        if (this.chunk < other.chunk)
            return -1;
        if (this.chunk > other.chunk)
            return 1;
        return Long.compare(this.position, other.position);
    }
}
