package com.github.sepgh.internal.tree.node;

import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.TreeNodeUtils;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Setter;

import java.util.Iterator;
import java.util.List;

/*
  Structure of a node in binary for non-leaf
  [1 byte -6 empty bits- IS_ROOT | IS_LEAF] + [POINTER_SIZE bytes child] + (([LONG_SIZE bytes id] + [POINTER_SIZE bytes child]) * max node size)

  Structure of a node in binary for leaf
  [1 byte -6 empty bits- IS_ROOT | IS_LEAF] + (([LONG_SIZE bytes id] + [POINTER_SIZE bytes data]) * max node size) + [POINTER_SIZE bytes previous leaf node] + [POINTER_SIZE bytes next leaf node]
*/
@Getter
public abstract class BaseTreeNode {
    public static byte TYPE_LEAF_NODE_BIT = 0x02; // 0 0 1 0
    public static byte TYPE_INTERNAL_NODE_BIT = 0x01; // 0 0 0 1
    public static byte ROOT_BIT = 0x04; // 0 1 0 0

    @Setter
    private Pointer pointer;
    private final byte[] data;
    @Getter
    private boolean modified = false;

    public BaseTreeNode(byte[] data) {
        this.data = data;
    }

    public boolean isLeaf(){   //  0 0  &  0 0
        return (data[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT;
    }

    protected void modified(){
        this.modified = true;
    }


    public static BaseTreeNode fromNodeData(IndexStorageManager.NodeData nodeData) {
        BaseTreeNode treeNode = BaseTreeNode.fromBytes(nodeData.bytes());
        treeNode.setPointer(nodeData.pointer());
        return treeNode;
    }
    public static BaseTreeNode fromBytes(byte[] data, Type type){
        if (type == Type.INTERNAL){
            return new InternalTreeNode(data);
        }
        return new LeafTreeNode(data);
    }

    public static BaseTreeNode fromBytes(byte[] data){
        if ((data[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
            return new LeafTreeNode(data);
        return new InternalTreeNode(data);
    }

    public byte[] toBytes(){
        return data;
    }

    public void setType(Type type) {
        modified();
        // Only can be called if the node is empty, otherwise changing type of already constructed node will F things up
        this.data[0] = (byte) (data[0] | type.getSign());
    }

    public Type getType(){
        if ((data[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
            return Type.LEAF;
        if ((data[0] & TYPE_INTERNAL_NODE_BIT) == TYPE_INTERNAL_NODE_BIT)
            return Type.INTERNAL;
        return null;
    }

    public void setAsRoot(){
        modified();
        this.data[0] = (byte) (data[0] | ROOT_BIT);
    }

    public void unsetAsRoot(){
        modified();
        this.data[0] = (byte) (data[0] & ~ROOT_BIT);
    }

    public Iterator<Long> getKeys(){
        return new TreeNodeKeysIterator(this);
    }

    public List<Long> getKeyList(){
        return ImmutableList.copyOf(getKeys());
    }

    public boolean isRoot() {
        return (data[0] & ROOT_BIT) == ROOT_BIT;
    }

    public enum Type {
        LEAF(TYPE_LEAF_NODE_BIT), INTERNAL(TYPE_INTERNAL_NODE_BIT);
        private final byte sign;

        Type(byte sign) {
            this.sign = sign;
        }

        public byte getSign() {
            return sign;
        }
    }

    private static class TreeNodeKeysIterator implements Iterator<Long> {
        private final BaseTreeNode node;
        private int cursor; // Cursor points to current key index, not current byte
        private boolean hasNext = true;

        private TreeNodeKeysIterator(BaseTreeNode node) {
            this.node = node;
        }

        @Override
        public boolean hasNext() {
            if (!hasNext)
                return false;

            hasNext = TreeNodeUtils.hasKeyAtIndex(this.node, cursor);
            return hasNext;
        }

        @Override
        public Long next() {
            long value = TreeNodeUtils.getKeyAtIndex(this.node, cursor);
            cursor++;
            return value;
        }
    }
}
