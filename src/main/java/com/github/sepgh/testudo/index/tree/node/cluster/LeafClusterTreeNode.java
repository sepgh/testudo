package com.github.sepgh.testudo.index.tree.node.cluster;

import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.tree.node.data.PointerIndexBinaryObject;

public class LeafClusterTreeNode<K extends Comparable<K>> extends AbstractLeafTreeNode<K, Pointer> {
    public LeafClusterTreeNode(byte[] data, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory) {
        super(data, keyIndexBinaryObjectFactory, new PointerIndexBinaryObject.Factory());
    }
}