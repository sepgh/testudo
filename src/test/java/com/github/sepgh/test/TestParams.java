package com.github.sepgh.test;

import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.PointerImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.utils.KVSize;

public class TestParams {
    public static final KVSize DEFAULT_KV_SIZE =  new KVSize(LongImmutableBinaryObjectWrapper.BYTES, PointerImmutableBinaryObjectWrapper.BYTES);
}
