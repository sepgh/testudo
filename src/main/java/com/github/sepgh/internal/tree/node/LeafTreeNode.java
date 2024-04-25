package com.github.sepgh.internal.tree.node;

import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.TreeNodeUtils;
import com.github.sepgh.internal.tree.exception.IllegalNodeAccess;

import java.util.Iterator;
import java.util.Map;

public class LeafTreeNode extends AbstractTreeNode {
    public LeafTreeNode(byte[] data) {
        super(data);
        assert data[0] == AbstractTreeNode.TYPE_LEAF_NODE;
    }

    public Iterator<Map.Entry<Long, Pointer>> keyValues(){
        return new TreeNodeKeyValueIterator(this);
    }

    public void setKeyValue(int index, long identifier, Pointer pointer) throws IllegalNodeAccess {
        TreeNodeUtils.setKeyValueAtIndex(this, index, identifier, pointer);
    }

    private static class TreeNodeKeyValueIterator implements Iterator<Map.Entry<Long, Pointer>> {
        private final AbstractTreeNode node;
        private int cursor; // Cursor points to current key index, not current byte
        private boolean hasNext = true;

        private TreeNodeKeyValueIterator(AbstractTreeNode node) {
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

}
