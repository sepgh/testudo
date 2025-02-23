package com.github.sepgh.testudo.index.data;

import com.github.sepgh.testudo.ds.Pointer;
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
        public IndexBinaryObject<Pointer> create(Pointer pointer) {
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

        @Override
        public Class<Pointer> getType() {
            return Pointer.class;
        }

        @Override
        public IndexBinaryObject<Pointer> createEmpty() {
            return this.create(Pointer.empty());
        }
    }
}
