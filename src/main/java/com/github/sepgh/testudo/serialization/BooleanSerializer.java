package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.scheme.Scheme;

import java.util.List;

public class BooleanSerializer implements Serializer<Boolean> {
    public static final String TYPE_NAME = FieldType.BOOLEAN.getName();

    @Override
    public Class<Boolean> getType() {
        return Boolean.TYPE;
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public List<String> compatibleTypes() {
        return List.of();
    }

    @Override
    public int maxSize() {
        return 1;
    }

    @Override
    public int minSize() {
        return 1;
    }

    @Override
    public byte[] serialize(Boolean s, Scheme.Meta meta) throws SerializationException {
        return new byte[]{(byte) (s ? 1 : 0)};
    }

    @Override
    public int getSize(Scheme.Meta meta) {
        return 1;
    }

    @Override
    public byte[] serializeDefault(String defaultValue, Scheme.Meta meta) throws SerializationException {
        if (defaultValue == null) {
            return serialize((Boolean) null, meta);
        } else if (defaultValue.equals("true") || defaultValue.equals("T") || defaultValue.equals("1")) {
            return serialize(true, meta);
        } else if (defaultValue.equals("false") || defaultValue.equals("F") || defaultValue.equals("0")) {
            return serialize(false, meta);
        }
        throw new SerializationException("Default value '" + defaultValue + "' is not supported");
    }

    @Override
    public Boolean deserialize(byte[] bytes, Scheme.Meta meta) throws DeserializationException {
        return (bytes[0] == (byte) 1);
    }
}
