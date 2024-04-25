package com.github.sepgh.internal.tree.node;

import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.TreeNodeUtils;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Setter;

import java.util.Iterator;
import java.util.List;

// Todo: This isn't reactive, but should it be?
// Todo: If AsyncFileChannels are used, do we need to use Futures here? (probably not?)

/*
  Structure of a node in binary for non-leaf
  [1 byte IS_LEAF] + [POINTER_SIZE bytes child] + (([LONG_SIZE bytes id] + [POINTER_SIZE bytes child]) * max node size)

  Structure of a node in binary for leaf
  [1 byte IS_LEAF] + (([LONG_SIZE bytes id] + [POINTER_SIZE bytes data]) * max node size) + [POINTER_SIZE bytes previous leaf node] + [POINTER_SIZE bytes next leaf node]
*/
@Getter
public abstract class AbstractTreeNode {
    public static byte TYPE_LEAF_NODE = 0x01;
    public static byte TYPE_INTERNAL_NODE = 0x02;

    @Setter
    private Pointer nodePointer;
    private final byte[] data;

    public AbstractTreeNode(byte[] data) {
        this.data = data;
    }

    public boolean isLeaf(){
        return data[0] == TYPE_LEAF_NODE;
    }

    public static AbstractTreeNode fromBytes(byte[] data){
        if (data[0] == TYPE_LEAF_NODE)
            return new LeafTreeNode(data);
        return new InternalTreeNode(data);
    }

    public byte[] toBytes(){
        return data;
    }

    public void setType(byte type) {
        this.data[0] = type;
    }

    public Iterator<Long> keys(){
        return new TreeNodeKeysIterator(this);
    }

    public List<Long> keyList(){
        return ImmutableList.copyOf(keys());
    }

    public int addKey(long key) {
        return TreeNodeUtils.addKeyAndGetIndex(this, key);
    }

    private static class TreeNodeKeysIterator implements Iterator<Long> {
        private final AbstractTreeNode node;
        private int cursor; // Cursor points to current key index, not current byte
        private boolean hasNext = true;

        private TreeNodeKeysIterator(AbstractTreeNode node) {
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
