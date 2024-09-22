package com.github.sepgh.testudo.index.tree.node;

import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.TreeNodeUtils;
import com.github.sepgh.testudo.utils.KVSize;
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
public abstract class AbstractTreeNode<K extends Comparable<K>> {
    public static byte TYPE_LEAF_NODE_BIT = 0x02; // 0 0 1 0
    public static byte TYPE_INTERNAL_NODE_BIT = 0x01; // 0 0 0 1
    public static byte ROOT_BIT = 0x04; // 0 1 0 0

    @Setter
    private Pointer pointer;
    private final byte[] data;
    @Getter
    private boolean modified = false;
    protected final IndexBinaryObjectFactory<K> kIndexBinaryObjectFactory;

    public AbstractTreeNode(byte[] data, IndexBinaryObjectFactory<K> kIndexBinaryObjectFactory) {
        this.data = data;
        this.kIndexBinaryObjectFactory = kIndexBinaryObjectFactory;
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

    public Iterator<K> getKeys(int degree, int valueSize){
        return new TreeNodeKeysIterator<K>(this, degree, valueSize);
    }

    public List<K> getKeyList(int degree, int valueSize){
        return ImmutableList.copyOf(getKeys(degree, valueSize));
    }

    public void setKey(int index, K key, int valueSize) throws IndexBinaryObject.InvalidIndexBinaryObject {
        TreeNodeUtils.setKeyAtIndex(this, index, kIndexBinaryObjectFactory.create(key), valueSize);
    }

    public KVSize getKVSize(){
        return new KVSize(kIndexBinaryObjectFactory.size(), PointerIndexBinaryObject.BYTES);
    }

    @SneakyThrows
    public void removeKey(int idx, int degree, int valueSize) {
        List<K> keyList = this.getKeyList(degree, valueSize);
        TreeNodeUtils.removeKeyAtIndex(this, idx, kIndexBinaryObjectFactory.size(), valueSize);
        List<K> subList = keyList.subList(idx + 1, keyList.size());
        int lastIndex = -1;
        for (int i = 0; i < subList.size(); i++) {
            lastIndex = idx + i;
            TreeNodeUtils.setKeyAtIndex(
                    this,
                    lastIndex,
                    kIndexBinaryObjectFactory.create(subList.get(i)),
                    valueSize
            );
        }
        if (lastIndex != -1){
            for (int i = lastIndex + 1; i < degree - 1; i++){
                TreeNodeUtils.removeKeyAtIndex(this, i, kIndexBinaryObjectFactory.size(), valueSize);
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

    private static class TreeNodeKeysIterator<K extends Comparable<K>> implements Iterator<K> {
        private final AbstractTreeNode<K> node;
        private final int degree;
        private final int valueSize;
        private int cursor; // Cursor points to current key index, not current byte
        private boolean hasNext = true;

        private TreeNodeKeysIterator(AbstractTreeNode<K> node, int degree, int valueSize) {
            this.node = node;
            this.degree = degree;
            this.valueSize = valueSize;
        }

        @SneakyThrows
        @Override
        public boolean hasNext() {
            if (!hasNext)
                return false;

            hasNext = TreeNodeUtils.hasKeyAtIndex(this.node, cursor, degree, this.node.kIndexBinaryObjectFactory, valueSize);
            return hasNext;
        }

        @SneakyThrows
        @Override
        public K next() {
            IndexBinaryObject<K> indexBinaryObject = TreeNodeUtils.getKeyAtIndex(this.node, cursor, this.node.kIndexBinaryObjectFactory, valueSize);
            cursor++;
            return indexBinaryObject.asObject();
        }
    }
}
