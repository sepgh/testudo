package com.github.sepgh.internal.index.tree.node.cluster;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.data.Identifier;
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
public class BaseClusterTreeNode extends AbstractTreeNode {

    public BaseClusterTreeNode(byte[] data) {
        super(data);
    }

    public static BaseClusterTreeNode fromNodeData(IndexStorageManager.NodeData nodeData) {
        BaseClusterTreeNode treeNode = BaseClusterTreeNode.fromBytes(nodeData.bytes());
        treeNode.setPointer(nodeData.pointer());
        return treeNode;
    }
    public static BaseClusterTreeNode fromBytes(byte[] data, Type type){
        if (type == Type.INTERNAL){
            return new InternalClusterTreeNode(data);
        }
        return new LeafClusterTreeNode(data);
    }

    public static BaseClusterTreeNode fromBytes(byte[] data){
        if ((data[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
            return new LeafClusterTreeNode(data);
        return new InternalClusterTreeNode(data);
    }

    public Iterator<Long> getKeys(int degree){
        return super.getKeys(degree, Identifier.class, Identifier.BYTES, Pointer.BYTES);
    }

    public List<Long> getKeyList(int degree){
        return ImmutableList.copyOf(getKeys(degree));
    }

    public void setKey(int index, long key){
        super.setKey(index, new Identifier(key), Identifier.BYTES, Pointer.BYTES);
    }

    public void removeKey(int idx, int degree) {
        super.removeKey(idx, degree, Identifier.class, Long.class, Identifier.BYTES, Pointer.BYTES);
    }

}
