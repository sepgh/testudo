package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.scheme.Scheme;
import lombok.SneakyThrows;

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
        return new IndexBinaryObjectFactory<T>() {

            @SneakyThrows   // Todo
            @Override
            public IndexBinaryObject<T> create(T t) throws IndexBinaryObject.InvalidIndexBinaryObject {
                return new IndexBinaryObjectSerializer<>(t, SERIALIZER);
            }

            @Override
            public IndexBinaryObject<T> create(byte[] bytes, int beginning) {
                return new IndexBinaryObjectSerializer<>(bytes, beginning, SERIALIZER);
            }

            @Override
            public int size() {
                return SERIALIZER.getSize(field.getMeta());
            }

            @Override
            public Class<T> getType() {
                return SERIALIZER.getType();
            }
        };
    }

    default String asString(byte[] bytes, Scheme.Meta meta) throws DeserializationException {
        return deserialize(bytes, meta).toString();
    }

    default String asString(T t) {
        return t.toString();
    }

    byte[] serializeDefault(String defaultValue, Scheme.Meta meta) throws SerializationException;
}
