package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.scheme.Scheme;

import java.util.List;

public interface Serializer<T extends Comparable<T>> {
    Class<T> getType();
    String typeName();
    List<String> compatibleTypes();
    int maxSize();
    int minSize();

    default byte[] serialize(T object) throws SerializationException {
        return this.serialize(object, Scheme.ImmutableDefaultMeta.INSTANCE);
    }

    default T deserialize(byte[] bytes) throws DeserializationException {
        return this.deserialize(bytes, Scheme.ImmutableDefaultMeta.INSTANCE);
    }

    byte[] serialize(T t, Scheme.Meta meta) throws SerializationException;

    T deserialize(byte[] bytes, Scheme.Meta meta) throws DeserializationException;

    default int getSize() {
        return this.getSize(Scheme.ImmutableDefaultMeta.INSTANCE);
    }

    int getSize(Scheme.Meta meta);

    default IndexBinaryObjectFactory<T> getIndexBinaryObjectFactory(Scheme.Field field) {
        Serializer<T> SERIALIZER = this;
        return new SerializerIndexBinaryObjectFactory<>(SERIALIZER, field);
    }

    default String asString(byte[] bytes, Scheme.Meta meta) throws DeserializationException {
        return deserialize(bytes, meta).toString();
    }

    default String asString(T t) {
        return t.toString();
    }

    byte[] serializeDefault(String defaultValue, Scheme.Meta meta) throws SerializationException;
}
