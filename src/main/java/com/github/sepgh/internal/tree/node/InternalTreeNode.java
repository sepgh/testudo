package com.github.sepgh.internal.tree.node;

import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.TreeNodeUtils;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class InternalTreeNode extends BaseTreeNode {
    public InternalTreeNode(byte[] data) {
        super(data);
        setType(NodeType.INTERNAL);
    }

    public void setChildAtIndex(int index, Pointer pointer) {
        TreeNodeUtils.setPointerToChild(this, index, pointer);
    }

    public Optional<Pointer> getChildAtIndex(int index){
        return Optional.of(TreeNodeUtils.getChildPointerAtIndex(this, index));
    }

    public Iterator<Pointer> children(){
        return new TreeNodeChildrenIterator(this);
    }

    public List<Pointer> childrenList(){
        return ImmutableList.copyOf(children());
    }

    public void removeChildAtIndex(int index) {
        TreeNodeUtils.removeChildAtIndex(this, index);
    }

    public void removeKeyAtIndex(int index) {
        TreeNodeUtils.removeKeyAtIndex(this, index);
    }

    public void setKeyAtIndex(int index, Long identifier) {
        TreeNodeUtils.setKeyAtIndex(this, index, identifier);
    }

    private static class TreeNodeChildrenIterator implements Iterator<Pointer> {

        private final BaseTreeNode node;
        private int cursor;  // Cursor points to current pointer index, not current byte
        private boolean hasNext = true;

        public TreeNodeChildrenIterator(BaseTreeNode node) {
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
            Pointer pointer = TreeNodeUtils.getChildPointerAtIndex(this.node, this.cursor);
            this.cursor++;
            return pointer;
        }
    }
}
