package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.utils.BinaryUtils;

import java.util.AbstractMap;
import java.util.Map;

public class TreeNodeUtils {
    private static final int TREE_NODE_FLAGS_END = 1;

    /**
     * @return bool true, if the first byte of the current cursor position is not 0x00
     * Note that index is the index of the pointer object we want to refer to, not the byte position in byte array
     */
    public static boolean hasChildPointerAtIndex(TreeNode treeNode, int index){
        if (TREE_NODE_FLAGS_END + (index * (Pointer.POINTER_SIZE + Long.BYTES)) + Pointer.POINTER_SIZE > treeNode.getData().length)
            return false;

        if (!treeNode.isLeaf()){
            return treeNode.getData()[TREE_NODE_FLAGS_END + (index * (Pointer.POINTER_SIZE + Long.BYTES))] == Pointer.TYPE_NODE;
        }

        return false;
    }

    public static Pointer getPointerAtIndex(TreeNode treeNode, int index){
        return Pointer.fromByteArray(treeNode.getData(), TREE_NODE_FLAGS_END + (index * (Pointer.POINTER_SIZE + Long.BYTES)));
    }

    private static int getKeyStartIndex(TreeNode treeNode, int index) {
        if (!treeNode.isLeaf()){
            return TREE_NODE_FLAGS_END + Pointer.POINTER_SIZE + (index * (Long.BYTES + Pointer.POINTER_SIZE));
        } else {
            return TREE_NODE_FLAGS_END + (index * (Long.BYTES + Pointer.POINTER_SIZE));
        }
    }

    public static boolean hasKeyAtIndex(TreeNode treeNode, int index){
        int keyStartIndex = getKeyStartIndex(treeNode, index);
        if (keyStartIndex + Long.BYTES > treeNode.getData().length)
            return false;

        return BinaryUtils.bytesToLong(treeNode.getData(), keyStartIndex) != 0;
    }

    public static long getKeyAtIndex(TreeNode treeNode, int index) {
        int keyStartIndex = getKeyStartIndex(treeNode, index);
        return BinaryUtils.bytesToLong(treeNode.getData(), keyStartIndex);
    }

    public static boolean hasKeyValuePointerAtIndex(TreeNode treeNode, int index){
        int keyStartIndex = getKeyStartIndex(treeNode, index);
        return keyStartIndex + Long.BYTES + Pointer.POINTER_SIZE <= treeNode.getData().length &&
                treeNode.getData()[keyStartIndex + Long.BYTES] == Pointer.TYPE_DATA;
    }

    public static Map.Entry<Long, Pointer> getKeyValuePointerAtIndex(TreeNode treeNode, int index) {
        int keyStartIndex = getKeyStartIndex(treeNode, index);
        return new AbstractMap.SimpleImmutableEntry<>(
            BinaryUtils.bytesToLong(treeNode.getData(), keyStartIndex),
            Pointer.fromByteArray(treeNode.getData(), keyStartIndex + Long.BYTES)
        );
    }

}
