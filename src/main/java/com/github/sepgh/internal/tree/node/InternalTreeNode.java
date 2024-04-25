package com.github.sepgh.internal.tree.node;

import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.TreeNodeUtils;
import com.github.sepgh.internal.tree.exception.IllegalNodeAccess;

import java.util.Iterator;
import java.util.Optional;

public class InternalTreeNode extends AbstractTreeNode {
    public InternalTreeNode(byte[] data) {
        super(data);
    }

    public void setChildAtIndex(int index, Pointer pointer) throws IllegalNodeAccess {
        TreeNodeUtils.setPointerToChild(this, index, pointer);
    }

    public Optional<Pointer> getChildAtIndex(int index){
        return Optional.of(TreeNodeUtils.getPointerAtIndex(this, index));
    }

    public Iterator<Pointer> children(){
        return new TreeNodeChildrenIterator(this);
    }

    private static class TreeNodeChildrenIterator implements Iterator<Pointer> {

        private final AbstractTreeNode node;
        private int cursor;  // Cursor points to current pointer index, not current byte
        private boolean hasNext = true;

        public TreeNodeChildrenIterator(AbstractTreeNode node) {
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
