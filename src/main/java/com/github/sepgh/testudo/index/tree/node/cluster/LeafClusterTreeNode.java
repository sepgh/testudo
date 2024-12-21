package com.github.sepgh.testudo.index.tree.node.cluster;

import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;

public class LeafClusterTreeNode<K extends Comparable<K>> extends AbstractLeafTreeNode<K, Pointer> {
    public LeafClusterTreeNode(byte[] data, IndexBinaryObjectFactory<K> keyIndexBinaryObjectFactory) {
        super(data, keyIndexBinaryObjectFactory, new PointerIndexBinaryObject.Factory());
    }
}