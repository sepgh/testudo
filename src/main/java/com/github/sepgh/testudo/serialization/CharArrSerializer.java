package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.StringImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.scheme.Scheme;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class CharArrSerializer implements Serializer<String> {
    public static final String TYPE_NAME = FieldType.CHAR.getName();
    public static int MAX_LENGTH = 512;

    @Override
    public Class<String> getType() {
        return String.class;
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
        return MAX_LENGTH;
    }

    @Override
    public int minSize() {
        return 0;
    }

    @Override
    public byte[] serialize(String s, Scheme.Meta meta) throws SerializationException {
        try {
            byte[] bytes = s.getBytes(meta.getCharset());
            if (bytes.length > maxSize()) {
                throw new SerializationException("String too long for charset " + meta.getCharset() + ", length: " + bytes.length);
            }

            if (meta.getMaxSize() != null && bytes.length > Integer.parseInt(meta.getMaxSize())) {
                throw new SerializationException("String is longer than max size defined in meta: " + meta.getMaxSize());
            }

            return bytes;
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public String deserialize(byte[] bytes, Scheme.Meta meta) throws DeserializationException {
        if (bytes.length > maxSize()) {
            throw new DeserializationException("String too long for deserialization. length: " + bytes.length);
        }
        try {
            return new String(bytes, meta.getCharset());
        } catch (UnsupportedEncodingException e) {
            throw new DeserializationException(e);
        }
    }

    @Override
    public int getSize(Scheme.Meta meta) {
        return Integer.parseInt(meta.getMaxSize());
    }

    @Override
    public ImmutableBinaryObjectWrapper<String> getImmutableBinaryObjectWrapper(Scheme.Field field) {
        return new StringImmutableBinaryObjectWrapper(field.getMeta().getMax());
    }

    @Override
    public byte[] serializeDefault(String defaultValue, Scheme.Meta meta) throws SerializationException {
        return this.serialize(defaultValue, meta);
    }
}
