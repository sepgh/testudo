package com.github.sepgh.internal.index.tree.node.cluster;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.internal.index.tree.node.data.NodeData;

public class LeafClusterTreeNode<K extends Comparable<K>> extends AbstractLeafTreeNode<K, Pointer> {
    public LeafClusterTreeNode(byte[] data, NodeData.Strategy<K> keyStrategy) {
        super(data, keyStrategy, NodeData.Strategy.POINTER);
    }
}