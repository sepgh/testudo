package com.github.sepgh.testudo.index.data;

import com.github.sepgh.testudo.index.Pointer;
import lombok.Getter;

@Getter
public class PointerIndexBinaryObject extends AbstractIndexBinaryObject<Pointer> {
    public static int BYTES = Pointer.BYTES;

    public PointerIndexBinaryObject() {
    }

    public PointerIndexBinaryObject(byte[] bytes) {
        super(bytes);
    }

    @Override
    public Pointer asObject() {
        return Pointer.fromBytes(bytes);
    }

    @Override
    public int size() {
        return BYTES;
    }

    public static class Factory implements IndexBinaryObjectFactory<Pointer> {

        @Override
        public IndexBinaryObject<Pointer> create(Pointer pointer) throws InvalidIndexBinaryObject {
            return new PointerIndexBinaryObject(pointer.toBytes());
        }

        @Override
        public IndexBinaryObject<Pointer> create(byte[] bytes, int beginning) {
            byte[] value = new byte[BYTES];
            System.arraycopy(bytes, beginning, value, 0, Pointer.BYTES);
            return new PointerIndexBinaryObject(value);
        }

        @Override
        public int size() {
            return BYTES;
        }
    }
}
