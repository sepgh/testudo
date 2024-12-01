package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.scheme.Scheme;
import com.google.common.primitives.UnsignedLong;

import java.math.BigInteger;
import java.util.List;

import static com.github.sepgh.testudo.utils.BinaryUtils.toByteArray;

public class UnsignedLongSerializer implements Serializer<UnsignedLong> {
    @Override
    public Class<UnsignedLong> getType() {
        return UnsignedLong.class;
    }

    @Override
    public String typeName() {
        return FieldType.UNSIGNED_LONG.getName();
    }

    @Override
    public List<String> compatibleTypes() {
        return List.of(FieldType.LONG.getName(), FieldType.INT.getName());
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
    public byte[] serialize(UnsignedLong unsignedLong, Scheme.Meta meta) throws SerializationException {
        return toByteArray(unsignedLong);
    }

    @Override
    public UnsignedLong deserialize(byte[] bytes, Scheme.Meta meta) throws DeserializationException {
        BigInteger bigInteger = new BigInteger(1, bytes);
        return UnsignedLong.valueOf(bigInteger);
    }

    @Override
    public int getSize(Scheme.Meta meta) {
        return Long.BYTES;
    }

    @Override
    public byte[] serializeDefault(String defaultValue, Scheme.Meta meta) throws SerializationException {
        return new byte[Long.BYTES]; // Todo
    }

    @Override
    public IndexBinaryObjectFactory<UnsignedLong> getIndexBinaryObjectFactory(Scheme.Field field) {
        return new SerializerIndexBinaryObjectFactory<>(this, field, () -> UnsignedLong.valueOf(1L), i -> i.plus(UnsignedLong.ONE));
    }
}
