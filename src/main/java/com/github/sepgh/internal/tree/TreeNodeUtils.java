package com.github.sepgh.internal.tree;

public class TreeNodeUtils {

    /**
     * @return bool true, if the first byte of the current cursor position is not 0x00
     * Note that index is the index of the pointer object we want to refer to, not the byte position in byte array
     */
    public static boolean hasPointerAtIndex(TreeNode treeNode, int index){
        if (index == 0){
            return treeNode.getData()[0] != 0x00;
        }

        return treeNode.getData()[(index * (Pointer.POINTER_SIZE + Long.BYTES)) + Pointer.POINTER_SIZE] != 0x00;
    }

    public static Pointer getPointerAtIndex(TreeNode treeNode, int index){
        if (index == 0)
            return Pointer.fromByteArray(treeNode.getData());
        return Pointer.fromByteArray(treeNode.getData(), (index * (Pointer.POINTER_SIZE + Long.BYTES)) + Pointer.POINTER_SIZE);
    }

}
