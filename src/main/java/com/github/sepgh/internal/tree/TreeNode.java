package com.github.sepgh.internal.tree;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Iterator;
import java.util.Optional;

// Todo: This isn't reactive, but should it be?
// Todo: If AsyncFileChannels are used, do we need to use Futures here? (probably not?)

@AllArgsConstructor
public class TreeNode {
    @Getter
    private final byte[] data;
    @Getter
    private final int nodeSize;

    public Optional<TreeNode> getChildAtIndex(int index){
        return Optional.empty();  // Todo
    }

    public Iterator<TreeNode> children(){
        return new TreeNodeChildrenIterator(this);
    }

    public TreeNode getChild(int index){
        // todo
        return null;
    }

    private static class TreeNodeChildrenIterator implements Iterator<TreeNode> {

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

            hasNext = TreeNodeUtils.hasPointerAtIndex(this.node, cursor);
            return hasNext;
        }

        @Override
        public TreeNode next() {
            Pointer pointer = TreeNodeUtils.getPointerAtIndex(this.node, this.cursor);
            this.cursor++;
            return null;  // Todo: construct a node from pointer
        }
    }
}
