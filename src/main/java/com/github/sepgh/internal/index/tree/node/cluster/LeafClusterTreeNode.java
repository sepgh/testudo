package com.github.sepgh.internal.index.tree.node.cluster;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.internal.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.internal.index.tree.node.data.PointerImmutableBinaryObjectWrapper;

public class LeafClusterTreeNode<K extends Comparable<K>> extends AbstractLeafTreeNode<K, Pointer> {
    public LeafClusterTreeNode(byte[] data, ImmutableBinaryObjectWrapper<K> keyImmutableBinaryObjectWrapper) {
        super(data, keyImmutableBinaryObjectWrapper, new PointerImmutableBinaryObjectWrapper());
    }
}