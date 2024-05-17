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

    public Iterator<KeyPointers> getKeyPointers(int degree){
        return new KeyPointersIterator(this, degree);
    }

    public List<KeyPointers> getKeyPointersList(int degree){
        return ImmutableList.copyOf(getKeyPointers(degree));
    }

    public void setKeyPointers(List<KeyPointers> keyPointers, int degree, boolean cleanRest){
        modified();
        if (cleanRest)
            TreeNodeUtils.cleanChildrenPointers(this, degree);
        int i = 0;
        for (KeyPointers keyPointer : keyPointers) {
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

    public void addKeyPointers(long identifier, @Nullable Pointer left, @Nullable Pointer right, int degree, boolean clearForNull){
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

    public void addKeyPointers(KeyPointers keyPointers, int degree) {
        modified();
        this.addKeyPointers(keyPointers.key, keyPointers.left, keyPointers.right, degree, false);
    }

    public void addKeyPointers(KeyPointers keyPointers, int degree, boolean clearForNull){
        modified();
        this.addKeyPointers(keyPointers.key, keyPointers.left, keyPointers.right, degree, clearForNull);
    }


    /*
     *   When is this called? when an internal node wanted to add a new child pointer but there is no space left
     *   Wherever the new identifier should be added, we add a new KeyPointers,
     *            where the left would point to current left of the existing node at that index
     *            and right would be the new pointer
     *            and if newly added KeyPointers was not last item, we change the next item left to the pointer (new one's right)
     *
     *   The returned list first node should be passed to parent and the remaining should be stored in a new node
     */
    public List<KeyPointers> addAndSplit(long identifier, Pointer pointer, int degree){
        modified();
        int mid = (degree - 1) / 2;

        List<Long> keyList = new ArrayList<>(getKeyList(degree));
        keyList.add(identifier);
        keyList.sort(Long::compareTo);

        int i = keyList.indexOf(identifier);

        List<KeyPointers> keyPointersList = new ArrayList<>(getKeyPointersList(degree));

        keyPointersList.add(i, new KeyPointers(
                        0,
                        identifier,
                        keyPointersList.get(i-1).getRight(),
                        pointer  // Setting right pointer at index
                )
        );
        if (i + 1 < keyPointersList.size()){
            keyPointersList.get(i+1).setLeft(pointer);  // Setting left pointer of next key if not last key
        }

        List<KeyPointers> toKeep = keyPointersList.subList(0, mid + 1);
        this.setKeyPointers(toKeep, degree, true);

        List<KeyPointers> toPass = keyPointersList.subList(mid + 1, keyList.size());
        return toPass;
    }


    private class KeyPointersIterator implements Iterator<KeyPointers> {

        private int cursor = 0;
        private Pointer lastRightPointer;

        private final BaseTreeNode node;
        private final int degree;

        private KeyPointersIterator(BaseTreeNode node, int degree) {
            this.node = node;
            this.degree = degree;
        }


        @Override
        public boolean hasNext() {
            return TreeNodeUtils.hasKeyAtIndex(node, cursor, degree);
        }

        @Override
        public KeyPointers next() {
            long keyAtIndex = TreeNodeUtils.getKeyAtIndex(node, cursor);
            KeyPointers keyPointers = null;
            if (cursor == 0){
                keyPointers = new KeyPointers(
                        cursor,
                        keyAtIndex,
                        TreeNodeUtils.getChildPointerAtIndex(node, 0),
                        TreeNodeUtils.getChildPointerAtIndex(node, 1)
                );
            } else {
                keyPointers = new KeyPointers(
                        cursor,
                        keyAtIndex,
                        lastRightPointer,
                        TreeNodeUtils.getChildPointerAtIndex(node, cursor + 1)
                );
            }
            lastRightPointer = keyPointers.getRight();

            cursor++;
            return keyPointers;
        }
    }


    @AllArgsConstructor
    @Getter
    @Setter
    @ToString
    public class KeyPointers implements Comparable<KeyPointers> {

        private int index;
        private long key;
        private Pointer left;
        private Pointer right;

        @Override
        public int compareTo(KeyPointers o) {
            return Long.compare(this.key, o.getKey());
        }
    }
}
