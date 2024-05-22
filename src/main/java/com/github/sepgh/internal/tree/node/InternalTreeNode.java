package com.github.sepgh.internal.tree.node;

import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.TreeNodeUtils;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InternalTreeNode extends BaseTreeNode {
    public InternalTreeNode(byte[] data) {
        super(data);
        setType(Type.INTERNAL);
    }

    public Iterator<ChildPointers> getChildPointers(int degree){
        return new ChildPointersIterator(this, degree);
    }

    public List<ChildPointers> getChildPointersList(int degree){
        return ImmutableList.copyOf(getChildPointers(degree));
    }

    public void setChildPointers(List<ChildPointers> childPointers, int degree, boolean cleanRest){
        modified();
        if (cleanRest)
            TreeNodeUtils.cleanChildrenPointers(this, degree);
        int i = 0;
        for (ChildPointers keyPointer : childPointers) {
            keyPointer.setIndex(i);
            TreeNodeUtils.setKeyAtIndex(this, keyPointer.index, keyPointer.key);
            if (i == 0){
                TreeNodeUtils.setPointerToChild(this, 0, keyPointer.left);
                TreeNodeUtils.setPointerToChild(this, 1, keyPointer.right);
            } else {
                TreeNodeUtils.setPointerToChild(this, keyPointer.index + 1, keyPointer.right);
            }
            i++;
        }

    }

    public void addChildPointers(long identifier, @Nullable Pointer left, @Nullable Pointer right, int degree, boolean clearForNull){
        modified();
        int i = TreeNodeUtils.addKeyAndGetIndex(this, identifier, degree);
        if (left != null){
            TreeNodeUtils.setPointerToChild(this, i, left);
        }
        else if (clearForNull)
            TreeNodeUtils.removeChildAtIndex(this, i);
        if (right != null)
            TreeNodeUtils.setPointerToChild(this, i+1, right);
        else if (clearForNull)
            TreeNodeUtils.removeChildAtIndex(this, i + 1);
    }

    public void addChildPointers(ChildPointers childPointers, int degree) {
        modified();
        this.addChildPointers(childPointers.key, childPointers.left, childPointers.right, degree, false);
    }

    public void addChildPointers(ChildPointers childPointers, int degree, boolean clearForNull){
        modified();
        this.addChildPointers(childPointers.key, childPointers.left, childPointers.right, degree, clearForNull);
    }

    public Iterator<Pointer> getChildren() {
        return new ChildrenIterator(this);
    }

    public List<Pointer> getChildrenList(){
        return ImmutableList.copyOf(getChildren());
    }

    public void setChildAtIndex(int index, Pointer pointer){
        TreeNodeUtils.setPointerToChild(this, index, pointer);
    }

    public Pointer getChildAtIndex(int index) {
        return TreeNodeUtils.getChildPointerAtIndex(this, index);
    }


    /*
     *   When is this called? when an internal node wanted to add a new child pointer but there is no space left
     *   Wherever the new identifier should be added, we add a new ChildPointers,
     *            where the left would point to current left of the existing node at that index
     *            and right would be the new pointer
     *            and if newly added ChildPointers was not last item, we change the next item left to the pointer (new one's right)
     *
     *   The returned list first node should be passed to parent and the remaining should be stored in a new node
     */
    public List<ChildPointers> addAndSplit(long identifier, Pointer pointer, int degree){
        modified();
        int mid = (degree - 1) / 2;

        List<Long> keyList = new ArrayList<>(getKeyList(degree));
        keyList.add(identifier);
        keyList.sort(Long::compareTo);

        int i = keyList.indexOf(identifier);

        List<ChildPointers> childPointersList = new ArrayList<>(getChildPointersList(degree));

        childPointersList.add(i, new ChildPointers(
                        0,
                        identifier,
                        childPointersList.get(i-1).getRight(),
                        pointer  // Setting right pointer at index
                )
        );
        if (i + 1 < childPointersList.size()){
            childPointersList.get(i+1).setLeft(pointer);  // Setting left pointer of next key if not last key
        }

        List<ChildPointers> toKeep = childPointersList.subList(0, mid + 1);
        this.setChildPointers(toKeep, degree, true);

        List<ChildPointers> toPass = childPointersList.subList(mid + 1, keyList.size());
        return toPass;
    }

    public void setKeys(List<Long> childKeyList) {
        for (int i = 0; i < childKeyList.size(); i++){
            this.setKey(i, childKeyList.get(i));
        }
    }

    public void setChildren(ArrayList<Pointer> childPointers) {
        for (int i = 0; i < childPointers.size(); i++){
            this.setChildAtIndex(i, childPointers.get(i));
        }
    }

    public void removeChild(int idx) {
        TreeNodeUtils.removeChildAtIndex(this, idx);
    }

    private static class ChildrenIterator implements Iterator<Pointer> {

        private final BaseTreeNode node;
        private int cursor = 0;

        private ChildrenIterator(BaseTreeNode node) {
            this.node = node;
        }

        @Override
        public boolean hasNext() {
            return TreeNodeUtils.hasChildPointerAtIndex(this.node, cursor);
        }

        @Override
        public Pointer next() {
            Pointer pointer = TreeNodeUtils.getChildPointerAtIndex(this.node, cursor);
            cursor++;
            return pointer;
        }
    }

    private class ChildPointersIterator implements Iterator<ChildPointers> {

        private int cursor = 0;
        private Pointer lastRightPointer;

        private final BaseTreeNode node;
        private final int degree;

        private ChildPointersIterator(BaseTreeNode node, int degree) {
            this.node = node;
            this.degree = degree;
        }


        @Override
        public boolean hasNext() {
            return TreeNodeUtils.hasKeyAtIndex(node, cursor, degree);
        }

        @Override
        public ChildPointers next() {
            long keyAtIndex = TreeNodeUtils.getKeyAtIndex(node, cursor);
            ChildPointers childPointers = null;
            if (cursor == 0){
                childPointers = new ChildPointers(
                        cursor,
                        keyAtIndex,
                        TreeNodeUtils.getChildPointerAtIndex(node, 0),
                        TreeNodeUtils.getChildPointerAtIndex(node, 1)
                );
            } else {
                childPointers = new ChildPointers(
                        cursor,
                        keyAtIndex,
                        lastRightPointer,
                        TreeNodeUtils.getChildPointerAtIndex(node, cursor + 1)
                );
            }
            lastRightPointer = childPointers.getRight();

            cursor++;
            return childPointers;
        }
    }


    @AllArgsConstructor
    @Getter
    @Setter
    @ToString
    public class ChildPointers implements Comparable<ChildPointers> {

        private int index;
        private long key;
        private Pointer left;
        private Pointer right;

        @Override
        public int compareTo(ChildPointers o) {
            return Long.compare(this.key, o.getKey());
        }
    }
}
