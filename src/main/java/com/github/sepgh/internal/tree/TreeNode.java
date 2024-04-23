package com.github.sepgh.internal.tree;

import lombok.Getter;
import lombok.Setter;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyIterator;

// Todo: This isn't reactive, but should it be?
// Todo: If AsyncFileChannels are used, do we need to use Futures here? (probably not?)

/*
  Structure of a node in binary for non-leaf
  [1 byte IS_LEAF] + [POINTER_SIZE bytes child] + (([LONG_SIZE bytes id] + [POINTER_SIZE bytes child]) * max node size)

  Structure of a node in binary for leaf
  [1 byte IS_LEAF] + ([LONG_SIZE bytes id] + [POINTER_SIZE bytes data]) * max node size) + [POINTER_SIZE bytes previous leaf node] + [POINTER_SIZE bytes next leaf node]
*/
public class TreeNode {
    public static byte TYPE_LEAF_NODE = 0x01;

    @Setter
    @Getter
    private Pointer nodePointer;
    @Getter
    private final byte[] data;

    public TreeNode(byte[] data) {
        this.data = data;
    }

    public boolean isLeaf(){
        return getData()[0] == TYPE_LEAF_NODE;
    }

    public Optional<Pointer> getChildAtIndex(int index){
        if (this.isLeaf())
            return Optional.empty();

        return Optional.of(TreeNodeUtils.getPointerAtIndex(this, index));
    }

    public Iterator<Pointer> children(){
        if (this.isLeaf()){
            return emptyIterator();
        }
        return new TreeNodeChildrenIterator(this);
    }

    public Iterator<Long> keys(){
        return new TreeNodeKeysIterator(this);
    }

    public Optional<Iterator<Map.Entry<Long, Pointer>>> keyValues(){
        if (!this.isLeaf()){
            return Optional.empty();
        }
        return Optional.of(new TreeNodeKeyValueIterator(this));
    }

    private static class TreeNodeKeyValueIterator implements Iterator<Map.Entry<Long, Pointer>> {
        private final TreeNode node;
        private int cursor; // Cursor points to current key index, not current byte
        private boolean hasNext = true;

        private TreeNodeKeyValueIterator(TreeNode node) {
            this.node = node;
        }

        @Override
        public boolean hasNext() {
            if (!hasNext)
                return false;

            hasNext = TreeNodeUtils.hasKeyValuePointerAtIndex(this.node, cursor);
            return hasNext;
        }

        @Override
        public Map.Entry<Long, Pointer> next() {
            Map.Entry<Long, Pointer> value = TreeNodeUtils.getKeyValuePointerAtIndex(this.node, cursor);
            cursor++;
            return value;
        }
    }

    private static class TreeNodeKeysIterator implements Iterator<Long> {
        private final TreeNode node;
        private int cursor; // Cursor points to current key index, not current byte
        private boolean hasNext = true;

        private TreeNodeKeysIterator(TreeNode node) {
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

    private static class TreeNodeChildrenIterator implements Iterator<Pointer> {

        private final TreeNode node;
        private int cursor;  // Cursor points to current pointer index, not current byte
        private boolean hasNext = true;

        public TreeNodeChildrenIterator(TreeNode node) {
            this.node = node;
        }

        @Override
        public boolean hasNext() {
            if (!this.hasNext)
                return false;

            hasNext = TreeNodeUtils.hasChildPointerAtIndex(this.node, cursor);
            return hasNext;
        }

        @Override
        public Pointer next() {
            Pointer pointer = TreeNodeUtils.getPointerAtIndex(this.node, this.cursor);
            this.cursor++;
            return pointer;
        }
    }
}
