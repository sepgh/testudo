package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.utils.BinaryUtils;
import com.google.common.primitives.Longs;

import java.util.List;

public class LongSerializer implements Serializer<Long> {
    public static final String TYPE_NAME = FieldType.LONG.getName();

    @Override
    public Class<Long> getType() {
        return Long.class;
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public List<String> compatibleTypes() {
        return List.of(FieldType.INT.getName());
    }

    @Override
    public int maxSize() {
        return Long.BYTES;
    }

    @Override
    public int minSize() {
        return Long.BYTES;
    }

    @Override
    public byte[] serialize(Long aLong, Scheme.Meta meta) throws SerializationException {
        return Longs.toByteArray(aLong);
    }

    @Override
    public Long deserialize(byte[] bytes, Scheme.Meta meta) {
        return BinaryUtils.bytesToLong(bytes, 0);
    }

    @Override
    public int getSize(Scheme.Meta meta) {
        return Long.BYTES;
    }

    @Override
    public byte[] serializeDefault(String defaultValue, Scheme.Meta meta) throws SerializationException {
        return this.serialize(Long.parseLong(defaultValue), meta);
    }

    @Override
    public IndexBinaryObjectFactory<Long> getIndexBinaryObjectFactory(Scheme.Field field) {
        return new SerializerIndexBinaryObjectFactory<>(this, field, () -> 0L, i -> i + 1L);
    }
}
