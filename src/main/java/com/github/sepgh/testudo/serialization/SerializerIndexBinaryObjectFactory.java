package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.scheme.Scheme;
import lombok.SneakyThrows;

import java.util.function.Function;
import java.util.function.Supplier;

public class SerializerIndexBinaryObjectFactory<T extends Comparable<T>> implements IndexBinaryObjectFactory<T> {
    private final Serializer<T> serializer;
    private final Scheme.Field field;

    private final Supplier<T> getFirst;
    private final Function<T, T> getNext;

    public SerializerIndexBinaryObjectFactory(Serializer<T> serializer, Scheme.Field field, Supplier<T> getFirst, Function<T, T> getNext) {
        this.serializer = serializer;
        this.field = field;
        this.getFirst = getFirst;
        this.getNext = getNext;
    }

    public SerializerIndexBinaryObjectFactory(Serializer<T> serializer, Scheme.Field field) {
        this(serializer, field, null, null);
    }

    @SneakyThrows   // Todo
    @Override
    public IndexBinaryObject<T> create(T t) {
        return new SerializerIndexBinaryObject<>(t, serializer)
                .setGetFirst(getFirst).setGetNext(getNext);
    }

    @Override
    public IndexBinaryObject<T> create(byte[] bytes, int beginning) {
        return new SerializerIndexBinaryObject<>(bytes, beginning, serializer)
                .setGetFirst(getFirst).setGetNext(getNext);
    }

    @Override
    public int size() {
        return serializer.getSize(field.getMeta());
    }

    @Override
    public Class<T> getType() {
        return serializer.getType();
    }

    @Override
    public IndexBinaryObject<T> createEmpty() {
        if (getFirst != null) {
            return this.create(getFirst.get());
        }
        return this.create(new byte[size()]);
    }

}
