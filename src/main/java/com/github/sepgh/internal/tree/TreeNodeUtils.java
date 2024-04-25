package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Longs;

import java.util.AbstractMap;
import java.util.Map;

public class TreeNodeUtils {
    private static final int TREE_NODE_FLAGS_END = 1;

    /**
     * @return bool true, if the first byte of the current cursor position is not 0x00
     * Note that index is the index of the pointer object we want to refer to, not the byte position in byte array
     */
    public static boolean hasChildPointerAtIndex(AbstractTreeNode treeNode, int index){
        if (TREE_NODE_FLAGS_END + (index * (Pointer.POINTER_SIZE + Long.BYTES)) + Pointer.POINTER_SIZE > treeNode.getData().length)
            return false;

        if (!treeNode.isLeaf()){
            return treeNode.getData()[TREE_NODE_FLAGS_END + (index * (Pointer.POINTER_SIZE + Long.BYTES))] == Pointer.TYPE_NODE;
        }

        return false;
    }

    public static Pointer getPointerAtIndex(AbstractTreeNode treeNode, int index){
        return Pointer.fromByteArray(treeNode.getData(), TREE_NODE_FLAGS_END + (index * (Pointer.POINTER_SIZE + Long.BYTES)));
    }

    public static void setPointerToChild(AbstractTreeNode treeNode, int index, Pointer pointer){
        if (index == 0){
            System.arraycopy(pointer.toByteArray(), 0, treeNode.getData(), TREE_NODE_FLAGS_END, Pointer.POINTER_SIZE);
        } else {
            System.arraycopy(
                    pointer.toByteArray(),
                    0,
                    treeNode.getData(),
                    TREE_NODE_FLAGS_END + (index * (Pointer.POINTER_SIZE + Long.BYTES)),
                    Pointer.POINTER_SIZE
            );
        }
    }

    private static int getKeyStartIndex(AbstractTreeNode treeNode, int index) {
        if (!treeNode.isLeaf()){
            return TREE_NODE_FLAGS_END + Pointer.POINTER_SIZE + (index * (Long.BYTES + Pointer.POINTER_SIZE));
        } else {
            return TREE_NODE_FLAGS_END + (index * (Long.BYTES + Pointer.POINTER_SIZE));
        }
    }

    public static boolean hasKeyAtIndex(AbstractTreeNode treeNode, int index){
        int keyStartIndex = getKeyStartIndex(treeNode, index);
        if (keyStartIndex + Long.BYTES > treeNode.getData().length)
            return false;

        return BinaryUtils.bytesToLong(treeNode.getData(), keyStartIndex) != 0;
    }

    public static long getKeyAtIndex(AbstractTreeNode treeNode, int index) {
        int keyStartIndex = getKeyStartIndex(treeNode, index);
        return BinaryUtils.bytesToLong(treeNode.getData(), keyStartIndex);
    }

    public static boolean hasKeyValuePointerAtIndex(AbstractTreeNode treeNode, int index){
        int keyStartIndex = getKeyStartIndex(treeNode, index);
        return keyStartIndex + Long.BYTES + Pointer.POINTER_SIZE <= treeNode.getData().length &&
                treeNode.getData()[keyStartIndex + Long.BYTES] == Pointer.TYPE_DATA;
    }

    public static Map.Entry<Long, Pointer> getKeyValuePointerAtIndex(AbstractTreeNode treeNode, int index) {
        int keyStartIndex = getKeyStartIndex(treeNode, index);
        return new AbstractMap.SimpleImmutableEntry<>(
            BinaryUtils.bytesToLong(treeNode.getData(), keyStartIndex),
            Pointer.fromByteArray(treeNode.getData(), keyStartIndex + Long.BYTES)
        );
    }

    public static void setKeyValueAtIndex(AbstractTreeNode treeNode, int index, long identifier, Pointer pointer) {
        System.arraycopy(
                Longs.toByteArray(identifier),
                0,
                treeNode.getData(),
                TREE_NODE_FLAGS_END + (index * (Long.BYTES + Pointer.POINTER_SIZE)),
                Long.BYTES
        );

        pointer.fillByteArray(
                treeNode.getData(),
                TREE_NODE_FLAGS_END + (index * (Long.BYTES + Pointer.POINTER_SIZE)) + Long.BYTES
        );
    }
}
