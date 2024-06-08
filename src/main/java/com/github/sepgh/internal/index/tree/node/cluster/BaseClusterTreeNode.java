package com.github.sepgh.internal.index.tree.node.cluster;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.data.LongIdentifier;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.Iterator;
import java.util.List;

/*
  Structure of a node in binary for non-leaf
  [1 byte -6 empty bits- IS_ROOT | IS_LEAF] + [POINTER_SIZE bytes child] + (([LONG_SIZE bytes id] + [POINTER_SIZE bytes child]) * max node size)

  Structure of a node in binary for leaf
  [1 byte -6 empty bits- IS_ROOT | IS_LEAF] + (([LONG_SIZE bytes id] + [POINTER_SIZE bytes data]) * max node size) + [POINTER_SIZE bytes previous leaf node] + [POINTER_SIZE bytes next leaf node]
*/
@Getter
public abstract class BaseClusterTreeNode<K extends Comparable<K>> extends AbstractTreeNode {
    protected final ClusterIdentifier.Strategy<K> clusterIdentifierStrategy;
    public BaseClusterTreeNode(byte[] data, ClusterIdentifier.Strategy<K> clusterIdentifierStrategy) {
        super(data);
        this.clusterIdentifierStrategy = clusterIdentifierStrategy;
    }

    public static <K extends Comparable<K>> BaseClusterTreeNode<K> fromNodeData(IndexStorageManager.NodeData nodeData, ClusterIdentifier.Strategy<K> strategy) {
        BaseClusterTreeNode<K> treeNode = BaseClusterTreeNode.fromBytes(nodeData.bytes(), strategy);
        treeNode.setPointer(nodeData.pointer());
        return treeNode;
    }
    public static <K extends Comparable<K>> BaseClusterTreeNode<K> fromBytes(byte[] data, Type type, ClusterIdentifier.Strategy<K> strategy){
        if (type == Type.INTERNAL){
            return new InternalClusterTreeNode<K>(data, strategy);
        }
        return new LeafClusterTreeNode<K>(data, strategy);
    }

    public static <K extends Comparable<K>> BaseClusterTreeNode<K> fromBytes(byte[] data, ClusterIdentifier.Strategy<K> strategy){
        if ((data[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
            return new LeafClusterTreeNode<K>(data, strategy);
        return new InternalClusterTreeNode<K>(data, strategy);
    }

    public Iterator<K> getKeys(int degree){
        return super.getKeys(degree, clusterIdentifierStrategy.getNodeInnerObjClass(), clusterIdentifierStrategy.size(), Pointer.BYTES);
    }

    public List<K> getKeyList(int degree){
        return ImmutableList.copyOf(getKeys(degree));
    }

    public void setKey(int index, K key){
        super.setKey(index, clusterIdentifierStrategy.fromObject(key), clusterIdentifierStrategy.size(), Pointer.BYTES);
    }

    public void removeKey(int idx, int degree) {
        super.removeKey(idx, degree, LongIdentifier.class, Long.class, clusterIdentifierStrategy.size(), Pointer.BYTES);
    }

}
