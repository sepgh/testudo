package com.github.sepgh.internal.tree.node;

import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.TreeNodeUtils;
import com.github.sepgh.internal.tree.exception.IllegalNodeAccess;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LeafTreeNode extends BaseTreeNode {
    public LeafTreeNode(byte[] data) {
        super(data);
        setType(NodeType.LEAF);
    }

    public Iterator<Map.Entry<Long, Pointer>> keyValues(){
        return new TreeNodeKeyValueIterator(this);
    }

    public void setKeyValue(int index, long key, Pointer pointer) throws IllegalNodeAccess {
        TreeNodeUtils.setKeyValueAtIndex(this, index, key, pointer);
    }

    public void removeKeyValueAtIndex(int index) {
        TreeNodeUtils.removeKeyValueAtIndex(this, index);
    }

    public int addKeyValue(long key, Pointer pointer) {
        return TreeNodeUtils.addKeyValueAndGetIndex(this, key, pointer);
    }

    public List<Map.Entry<Long, Pointer>> keyValueList(){
        return ImmutableList.copyOf(keyValues());
    }


    public void setPrevious(Pointer pointer) {
        TreeNodeUtils.setPreviousPointer(this, pointer);
    }

    public Optional<Pointer> getPrevious(){
        return TreeNodeUtils.getPreviousPointer(this);
    }

    public void setNext(Pointer pointer) {
        TreeNodeUtils.setNextPointer(this, pointer);
    }

    public Optional<Pointer> getNext(){
        return TreeNodeUtils.getNextPointer(this);
    }

    private static class TreeNodeKeyValueIterator implements Iterator<Map.Entry<Long, Pointer>> {
        private final BaseTreeNode node;
        private int cursor; // Cursor points to current key index, not current byte
        private boolean hasNext = true;

        private TreeNodeKeyValueIterator(BaseTreeNode node) {
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
