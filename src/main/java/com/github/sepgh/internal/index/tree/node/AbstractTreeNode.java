package com.github.sepgh.internal.index.tree.node;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.TreeNodeUtils;
import com.github.sepgh.internal.index.tree.node.data.NodeInnerObj;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

import java.util.Iterator;
import java.util.List;

/*
  Structure of a node in binary for non-leaf
  [1 byte -6 empty bits- IS_ROOT | IS_LEAF] + [POINTER_SIZE bytes child] + (([LONG_SIZE bytes id] + [POINTER_SIZE bytes child]) * max node size)

  Structure of a node in binary for leaf
  [1 byte -6 empty bits- IS_ROOT | IS_LEAF] + (([LONG_SIZE bytes id] + [POINTER_SIZE bytes data]) * max node size) + [POINTER_SIZE bytes previous leaf node] + [POINTER_SIZE bytes next leaf node]
*/
@Getter
public abstract class AbstractTreeNode {
    public static byte TYPE_LEAF_NODE_BIT = 0x02; // 0 0 1 0
    public static byte TYPE_INTERNAL_NODE_BIT = 0x01; // 0 0 0 1
    public static byte ROOT_BIT = 0x04; // 0 1 0 0

    @Setter
    private Pointer pointer;
    private final byte[] data;
    @Getter
    private boolean modified = false;

    public AbstractTreeNode(byte[] data) {
        this.data = data;
    }

    public boolean isLeaf(){   //  0 0  &  0 0
        return (data[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT;
    }

    protected void modified(){
        this.modified = true;
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

    public boolean isRoot() {
        return (data[0] & ROOT_BIT) == ROOT_BIT;
    }

    public <E extends Comparable<E>> Iterator<E> getKeys(int degree, Class<? extends NodeInnerObj<E>> nodeInnerObjectClass, int keySize, int valueSize){
        return new TreeNodeKeysIterator<E>(this, degree, nodeInnerObjectClass, keySize, valueSize);
    }

    public <E extends Comparable<E>> List<E> getKeyList(int degree, Class<? extends NodeInnerObj<E>> nodeInnerObjectClass, int keySize, int valueSize){
        return ImmutableList.copyOf(getKeys(degree, nodeInnerObjectClass, keySize, valueSize));
    }

    public <E extends Comparable<E>> void setKey(int index, NodeInnerObj<E> key, int keySize, int valueSize){
        TreeNodeUtils.setKeyAtIndex(this, index, key, valueSize);
    }

    @SneakyThrows
    public <E extends Comparable<E>> void removeKey(int idx, int degree, Class<? extends NodeInnerObj<E>> keyClass, Class<E> clazz, int keySize, int valueSize) {
        List<E> keyList = this.getKeyList(degree, keyClass, keySize, valueSize);
        TreeNodeUtils.removeKeyAtIndex(this, idx, keySize, valueSize);
        List<E> subList = keyList.subList(idx + 1, keyList.size());
        int lastIndex = -1;
        for (int i = 0; i < subList.size(); i++) {
            lastIndex = idx + i;
            TreeNodeUtils.setKeyAtIndex(
                    this,
                    lastIndex,
                    keyClass.getConstructor(clazz).newInstance(subList.get(i)),
                    valueSize
            );
        }
        if (lastIndex != -1){
            for (int i = lastIndex + 1; i < degree - 1; i++){
                TreeNodeUtils.removeKeyAtIndex(this, i, keySize, valueSize);
            }
        }
    }

    @Getter
    public enum Type {
        LEAF(TYPE_LEAF_NODE_BIT), INTERNAL(TYPE_INTERNAL_NODE_BIT);
        private final byte sign;

        Type(byte sign) {
            this.sign = sign;
        }
    }

    private static class TreeNodeKeysIterator<E extends Comparable<E>> implements Iterator<E> {
        private final AbstractTreeNode node;
        private final int degree;
        private final Class<? extends NodeInnerObj<E>> kClass;
        private final int keySize;
        private final int valueSize;
        private int cursor; // Cursor points to current key index, not current byte
        private boolean hasNext = true;

        private TreeNodeKeysIterator(AbstractTreeNode node, int degree, Class<? extends NodeInnerObj<E>> kClass, int keySize, int valueSize) {
            this.node = node;
            this.degree = degree;
            this.kClass = kClass;
            this.keySize = keySize;
            this.valueSize = valueSize;
        }

        @SneakyThrows
        @Override
        public boolean hasNext() {
            if (!hasNext)
                return false;

            hasNext = TreeNodeUtils.hasKeyAtIndex(this.node, cursor, degree, kClass, keySize, valueSize);
            return hasNext;
        }

        @SneakyThrows
        @Override
        public E next() {
            NodeInnerObj<E> nodeInnerObj = TreeNodeUtils.getKeyAtIndex(this.node, cursor, kClass, keySize, valueSize);
            cursor++;
            return nodeInnerObj.data();
        }
    }
}
