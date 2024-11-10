package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.scheme.Scheme;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
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
                throw new SerializationException("String (as bytes) is too long for charset " + meta.getCharset() + ", length: " + bytes.length);
            }

            if (meta.getMaxLength() > -1 && bytes.length > meta.getMaxLength()) {
                throw new SerializationException("String (as bytes) is longer than max size defined in meta: " + meta.getMaxLength() + ", length: " + bytes.length);
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
        if (meta.getMaxLength() == -1)
            return MAX_LENGTH;
        return meta.getMaxLength();
    }


    @Override
    public byte[] serializeDefault(String defaultValue, Scheme.Meta meta) throws SerializationException {
        return this.serialize(defaultValue, meta);
    }

    @Override
    public IndexBinaryObjectFactory<String> getIndexBinaryObjectFactory(Scheme.Field field) {
        return new CharArrIndexBinaryObjectFactory(this.getSize(field.getMeta()), this);
    }

    public static class CharArrIndexBinaryObjectFactory implements IndexBinaryObjectFactory<String> {
        private final int size;
        private final CharArrSerializer serializer;

        public CharArrIndexBinaryObjectFactory(int size, CharArrSerializer serializer) {
            this.size = size;
            this.serializer = serializer;
        }

        @Override
        public IndexBinaryObject<String> create(String s) {
            byte[] temp = s.getBytes(StandardCharsets.UTF_8);
            if (temp.length > size) {
                throw new RuntimeException("Fuck");  // Todo: proper exception to be thrown here. currently impossible
            }

            byte[] result = new byte[size];

            System.arraycopy(temp, 0, result, 0, temp.length);

            for (int i = temp.length; i < size; i++) {
                result[i] = 0;
            }

            return new IndexBinaryObjectSerializer<>(result, serializer);
        }

        @Override
        public IndexBinaryObject<String> create(byte[] bytes, int beginning) {
            byte[] data = new byte[size];
            System.arraycopy(
                    bytes,
                    beginning,
                    data,
                    0,
                    this.size()
            );
            return new IndexBinaryObjectSerializer<>(data, serializer);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Class<String> getType() {
            return String.class;
        }
    }
}
