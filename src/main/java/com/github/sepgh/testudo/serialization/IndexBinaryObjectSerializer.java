package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;

public class IndexBinaryObjectSerializer<T extends Comparable<T>> implements IndexBinaryObject<T> {
    private final byte[] bytes;
    private final Serializer<T> serializer;

    public IndexBinaryObjectSerializer(byte[] bytes, int beginning, Serializer<T> serializer) {
        this.serializer = serializer;
        byte[] copy = new byte[serializer.getSize()];
        System.arraycopy(bytes, beginning, copy, 0, serializer.getSize());
        this.bytes = copy;
    }

    public IndexBinaryObjectSerializer(T object, Serializer<T> serializer) throws SerializationException {
        this.bytes = serializer.serialize(object);
        this.serializer = serializer;
    }

    public IndexBinaryObjectSerializer(byte[] bytes, Serializer<T> serializer) {
        if (bytes.length > serializer.maxSize()) {
            this.bytes = new byte[serializer.maxSize()];
            System.arraycopy(bytes, 0, this.bytes, 0, serializer.getSize());
        } else {
            this.bytes = bytes;
        }
        this.serializer = serializer;
    }

    @Override
    public T asObject() {
        try {
            return serializer.deserialize(this.bytes);
        } catch (DeserializationException e) {
            throw new RuntimeException(e);  // Todo
        }
    }

    @Override
    public boolean hasValue() {
        return asObject() != null;
    }

    @Override
    public int size() {
        return bytes.length;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
