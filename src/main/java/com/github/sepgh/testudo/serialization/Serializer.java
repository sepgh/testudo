package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.scheme.Scheme;

import java.util.List;

public interface Serializer<T extends Comparable<T>> {
    Class<T> getType();
    String typeName();
    List<String> compatibleTypes();
    int maxSize();
    int minSize();
    byte[] serialize(T t, Scheme.Meta meta) throws SerializationException;
    T deserialize(byte[] bytes, Scheme.Meta meta) throws DeserializationException;
    int getSize(Scheme.Meta meta);
    ImmutableBinaryObjectWrapper<T> getImmutableBinaryObjectWrapper(Scheme.Field field);
    default String asString(byte[] bytes, Scheme.Meta meta) throws DeserializationException {
        return deserialize(bytes, meta).toString();
    }

    default String asString(T t) {
        return t.toString();
    }

    byte[] serializeDefault(String defaultValue, Scheme.Meta meta) throws SerializationException;
}
