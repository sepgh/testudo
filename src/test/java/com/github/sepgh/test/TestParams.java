package com.github.sepgh.test;

import com.github.sepgh.testudo.index.tree.node.data.LongIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.utils.KVSize;

public class TestParams {
    public static final KVSize DEFAULT_KV_SIZE =  new KVSize(LongIndexBinaryObject.BYTES, PointerIndexBinaryObject.BYTES);
}
