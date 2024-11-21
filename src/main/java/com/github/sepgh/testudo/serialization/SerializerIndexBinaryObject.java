package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.google.common.base.Preconditions;

import java.util.function.Function;
import java.util.function.Supplier;

public class SerializerIndexBinaryObject<T extends Comparable<T>> implements IndexBinaryObject<T> {
    protected final byte[] bytes;
    protected final Serializer<T> serializer;
    protected volatile T object;

    private Supplier<T> getFirst;
    private Function<T, T> getNext;

    public SerializerIndexBinaryObject(byte[] bytes, int beginning, Serializer<T> serializer) {
        this.serializer = serializer;
        byte[] copy = new byte[serializer.getSize()];
        System.arraycopy(bytes, beginning, copy, 0, serializer.getSize());
        this.bytes = copy;
    }

    public SerializerIndexBinaryObject(T object, Serializer<T> serializer) throws SerializationException {
        this.object = object;
        this.bytes = serializer.serialize(object);
        this.serializer = serializer;
    }

    public SerializerIndexBinaryObject(byte[] bytes, Serializer<T> serializer) {
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
        if (object != null)
            return object;
        try {
            return serializer.deserialize(this.bytes);
        } catch (DeserializationException e) {
            throw new RuntimeException(e);  // Todo
        }
    }

    @Override
    public int size() {
        return bytes.length;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public T getFirst() {
        Preconditions.checkNotNull(getFirst);
        return this.getFirst.get();
    }

    @Override
    public T getNext(T current) {
        Preconditions.checkNotNull(getNext);
        return this.getNext.apply(current);
    }

    public T getNext() {
        return getNext(asObject());
    }

    @Override
    public boolean supportIncrements() {
        return getFirst != null && getNext != null;
    }

    public SerializerIndexBinaryObject<T> setGetFirst(Supplier<T> getFirst) {
        this.getFirst = getFirst;
        return this;
    }

    public SerializerIndexBinaryObject<T> setGetNext(Function<T, T> getNext) {
        this.getNext = getNext;
        return this;
    }
}
