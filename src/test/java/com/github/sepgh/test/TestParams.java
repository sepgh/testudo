package com.github.sepgh.test;

import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.serialization.LongSerializer;
import com.github.sepgh.testudo.serialization.UnsignedLongSerializer;
import com.github.sepgh.testudo.ds.KVSize;
import com.google.common.primitives.UnsignedLong;

import java.util.function.Supplier;

public class TestParams {
    public static final KVSize DEFAULT_KV_SIZE =  new KVSize(Long.BYTES, PointerIndexBinaryObject.BYTES);
    public static final Supplier<Scheme.Field> FAKE_FIELD_SUPPLIER = () -> Scheme.Field.builder().build();
    public static final Scheme.Field FAKE_FIELD = FAKE_FIELD_SUPPLIER.get();
    public static final Supplier<IndexBinaryObjectFactory<Long>> LONG_INDEX_BINARY_OBJECT_FACTORY = () -> new LongSerializer().getIndexBinaryObjectFactory(FAKE_FIELD);
    public static final Supplier<IndexBinaryObjectFactory<Long>> DEFAULT_INDEX_BINARY_OBJECT_FACTORY = () -> new LongSerializer().getIndexBinaryObjectFactory(FAKE_FIELD);
    public static final Supplier<IndexBinaryObjectFactory<UnsignedLong>> ULONG_INDEX_BINARY_OBJECT_FACTORY = () -> new UnsignedLongSerializer().getIndexBinaryObjectFactory(FAKE_FIELD);
}
