package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Ints;

import java.util.List;

public class IntegerSerializer implements Serializer<Integer> {
    public static final String TYPE_NAME = FieldType.INT.getName();

    @Override
    public Class<Integer> getType() {
        return Integer.class;
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public List<String> compatibleTypes() {
        return List.of(FieldType.LONG.getName());
    }

    @Override
    public int maxSize() {
        return Integer.BYTES;
    }

    @Override
    public int minSize() {
        return Integer.BYTES;
    }

    @Override
    public byte[] serialize(Integer integer, Scheme.Meta meta) throws SerializationException {
        return Ints.toByteArray(integer);
    }

    @Override
    public Integer deserialize(byte[] bytes, Scheme.Meta meta) {
        return BinaryUtils.bytesToInteger(bytes, 0);
    }

    @Override
    public int getSize(Scheme.Meta meta) {
        return Integer.BYTES;
    }

    @Override
    public byte[] serializeDefault(String defaultValue, Scheme.Meta meta) throws SerializationException {
        return this.serialize(Integer.parseInt(defaultValue), meta);
    }

    @Override
    public IndexBinaryObjectFactory<Integer> getIndexBinaryObjectFactory(Scheme.Field field) {
        return new SerializerIndexBinaryObjectFactory<>(this, field, () -> 0, i -> i + 1);
    }
}
