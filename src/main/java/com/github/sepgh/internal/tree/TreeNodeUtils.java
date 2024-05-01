package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.utils.BinaryUtils;
import com.google.common.primitives.Longs;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;

public class TreeNodeUtils {
    private static final int OFFSET_TREE_NODE_FLAGS_END = 1;
    private static final int OFFSET_INTERNAL_NODE_KEY_BEGIN = OFFSET_TREE_NODE_FLAGS_END + Pointer.BYTES;
    private static final int OFFSET_LEAF_NODE_KEY_BEGIN = OFFSET_TREE_NODE_FLAGS_END;
    private static final int SIZE_INTERNAL_NODE_KEY_POINTER = Long.BYTES + Pointer.BYTES;
    private static final int SIZE_LEAF_NODE_KEY_POINTER = Long.BYTES + Pointer.BYTES;

    /**
     *
     * @param treeNode node to read/write from/to
     * @param index to check child pointer
     * @return bool true, if the first byte of the current cursor position is not 0x00
     *         Note that index is the index of the pointer object we want to refer to, not the byte position in byte array
     */
    public static boolean hasChildPointerAtIndex(BaseTreeNode treeNode, int index){
        if (OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + Long.BYTES)) > treeNode.getData().length)
            return false;


        return treeNode.getData()[OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + Long.BYTES))] == Pointer.TYPE_NODE;
    }


    /**
     * @param treeNode node to read/write from/to
     * @param index to check child pointer
     * @return Pointer to child node at index
     */public static Pointer getChildPointerAtIndex(BaseTreeNode treeNode, int index){
        return Pointer.fromByteArray(treeNode.getData(), OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + Long.BYTES)));
    }

    public static void removeChildAtIndex(BaseTreeNode treeNode, int index) {
        System.arraycopy(
                new byte[Pointer.BYTES],
                0,
                treeNode.getData(),
                OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + Long.BYTES)),
                Pointer.BYTES
        );
    }

    /**
     * @param treeNode node to read/write from/to
     * @param index index of the pointer to set
     * @param pointer object to set
     */
    public static void setPointerToChild(BaseTreeNode treeNode, int index, Pointer pointer){
        if (index == 0){
            System.arraycopy(pointer.toByteArray(), 0, treeNode.getData(), OFFSET_TREE_NODE_FLAGS_END, Pointer.BYTES);
        } else {
            System.arraycopy(
                    pointer.toByteArray(),
                    0,
                    treeNode.getData(),
                    OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + Long.BYTES)),
                    Pointer.BYTES
            );
        }
    }

    /**
     * @param treeNode to read/write from/to
     * @param index of the key we are looking for
     * @return the offset where the key is found at
     */
    private static int getKeyStartOffset(BaseTreeNode treeNode, int index) {
        if (!treeNode.isLeaf()){
            return OFFSET_INTERNAL_NODE_KEY_BEGIN + (index * (Long.BYTES + Pointer.BYTES));
        } else {
            return OFFSET_TREE_NODE_FLAGS_END + (index * (Long.BYTES + Pointer.BYTES));
        }
    }

    /**
     * @param treeNode to read/write from/to
     * @param index of the key to check existence
     * @return boolean state of existence of a key in index
     */
    public static boolean hasKeyAtIndex(BaseTreeNode treeNode, int index){
        int keyStartIndex = getKeyStartOffset(treeNode, index);
        if (keyStartIndex + Long.BYTES > treeNode.getData().length)
            return false;

        return BinaryUtils.bytesToLong(treeNode.getData(), keyStartIndex) != 0;
    }


    public static void setKeyAtIndex(BaseTreeNode treeNode, int index, Long identifier) {
        int keyStartIndex = getKeyStartOffset(treeNode, index);
        System.arraycopy(
                Longs.toByteArray(identifier),
                0,
                treeNode.getData(),
                keyStartIndex,
                Long.BYTES
        );
    }

    /**
     * @param treeNode to read/write from/to
     * @param index to read they key at
     * @return key value at index
     */
    public static long getKeyAtIndex(BaseTreeNode treeNode, int index) {
        int keyStartIndex = getKeyStartOffset(treeNode, index);
        return BinaryUtils.bytesToLong(treeNode.getData(), keyStartIndex);
    }

    public static void removeKeyAtIndex(BaseTreeNode treeNode, int index) {
        System.arraycopy(
                new byte[Long.BYTES],
                0,
                treeNode.getData(),
                getKeyStartOffset(treeNode, index),
                Long.BYTES
        );
    }

    public static boolean hasKeyValuePointerAtIndex(BaseTreeNode treeNode, int index){
        int keyStartIndex = getKeyStartOffset(treeNode, index);
        return keyStartIndex + SIZE_LEAF_NODE_KEY_POINTER <= treeNode.getData().length &&
                treeNode.getData()[keyStartIndex + Long.BYTES] == Pointer.TYPE_DATA;
    }

    public static Map.Entry<Long, Pointer> getKeyValuePointerAtIndex(BaseTreeNode treeNode, int index) {
        int keyStartIndex = getKeyStartOffset(treeNode, index);
        return new AbstractMap.SimpleImmutableEntry<>(
            BinaryUtils.bytesToLong(treeNode.getData(), keyStartIndex),
            Pointer.fromByteArray(treeNode.getData(), keyStartIndex + Long.BYTES)
        );
    }

    public static void setKeyValueAtIndex(BaseTreeNode treeNode, int index, long key, Pointer pointer) {
        System.arraycopy(
                Longs.toByteArray(key),
                0,
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + (index * (SIZE_LEAF_NODE_KEY_POINTER)),
                Long.BYTES
        );

        pointer.fillByteArrayWithPointer(
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + (index * (SIZE_LEAF_NODE_KEY_POINTER)) + Long.BYTES
        );
    }


    /**
     * @param treeNode to read/write from/to
     * @param key to add
     * @param pointer to add
     * @return index which key is added to
     * Todo: performance improvements may be possible
     *       linear search is used to sort the keys
     *       binary search could be used
     *       alternatively, we can hold a space for metadata which keeps track of the number of keys or values stored
     */
    public static int addKeyValueAndGetIndex(BaseTreeNode treeNode, long key, Pointer pointer) {

        int indexToFill = -1;
        for (int i = 0; i < ((treeNode.getData().length - 1 - (2 * Pointer.BYTES)) / (Long.BYTES + Pointer.BYTES)); i++){
            long keyAtIndex = getKeyAtIndex(treeNode, i);

            if (keyAtIndex == 0 || keyAtIndex > key){
                indexToFill = i;
                break;
            }
        }

        if (indexToFill == -1){
            return -1;
        }

        byte[] temp = new byte[treeNode.getData().length - (OFFSET_LEAF_NODE_KEY_BEGIN + (indexToFill * (SIZE_LEAF_NODE_KEY_POINTER)))];
        System.arraycopy(
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + (indexToFill * (SIZE_LEAF_NODE_KEY_POINTER)),
                temp,
                0,
                temp.length
        );

        setKeyValueAtIndex(treeNode, indexToFill, key, pointer);

        System.arraycopy(
                temp,
                0,
                treeNode.getData(),
                OFFSET_TREE_NODE_FLAGS_END + ((indexToFill + 1) * (SIZE_LEAF_NODE_KEY_POINTER)),
                temp.length - SIZE_LEAF_NODE_KEY_POINTER
        );


        return indexToFill;

    }

    public static void removeKeyValueAtIndex(BaseTreeNode treeNode, int index) {
        System.arraycopy(
                new byte[Long.BYTES + Pointer.BYTES],
                0,
                treeNode.getData(),
                getKeyStartOffset(treeNode, index),
                Long.BYTES + Pointer.BYTES
        );
    }

    /**
     * @param treeNode to read/write from/to
     * @param key to add
     * @return index which key is added to
     * Todo: performance improvements may be possible
     *       linear search is used to sort the keys
     *       binary search could be used
     *       alternatively, we can hold a space for metadata which keeps track of the number of keys or values stored
     */
    public static int addKeyAndGetIndex(BaseTreeNode treeNode, long key) {
        // Shall only be called on internal nodes

        int indexToFill = 0;
        long keyAtIndex = 0;
        for (int i = 0; i < ((treeNode.getData().length - 1) / (Long.BYTES + Pointer.BYTES)); i++){
            keyAtIndex = getKeyAtIndex(treeNode, i);

            if (keyAtIndex == 0 || key < keyAtIndex){
                indexToFill = i;
                break;
            }

        }

        if (keyAtIndex == 0){
            System.arraycopy(
                    Longs.toByteArray(key),
                    0,
                    treeNode.getData(),
                    OFFSET_INTERNAL_NODE_KEY_BEGIN + (indexToFill * (SIZE_LEAF_NODE_KEY_POINTER)),
                    Long.BYTES
            );
        } else {
            // Copy existing bytes to a temporary location
            byte[] temp = new byte[treeNode.getData().length - (OFFSET_TREE_NODE_FLAGS_END + (indexToFill * (SIZE_LEAF_NODE_KEY_POINTER)))];
            System.arraycopy(
                    treeNode.getData(),
                    OFFSET_INTERNAL_NODE_KEY_BEGIN + (indexToFill * (SIZE_LEAF_NODE_KEY_POINTER)),
                    temp,
                    0,
                    temp.length
            );

            // Write
            System.arraycopy(
                    Longs.toByteArray(key),
                    0,
                    treeNode.getData(),
                    OFFSET_INTERNAL_NODE_KEY_BEGIN + (indexToFill * (Long.BYTES + Pointer.BYTES)),
                    Long.BYTES
            );

            // Copy temp bytes back to valid position
            System.arraycopy(
                    temp,
                    0,
                    treeNode.getData(),
                    OFFSET_INTERNAL_NODE_KEY_BEGIN + ((indexToFill + 1) * (Long.BYTES + Pointer.BYTES)),
                    Long.BYTES
            );

        }

        return indexToFill;
    }

    public static Optional<Pointer> getPreviousPointer(BaseTreeNode treeNode){
        if (treeNode.getData()[treeNode.getData().length - (2*Pointer.BYTES)] == (byte) 0x0){
            return Optional.empty();
        }

        return Optional.of(
                Pointer.fromByteArray(treeNode.getData(), treeNode.getData().length - (2*Pointer.BYTES))
        );
    }

    public static void setPreviousPointer(BaseTreeNode treeNode, Pointer pointer) {
        System.arraycopy(
                pointer.toByteArray(),
                0,
                treeNode.getData(),
                treeNode.getData().length - (2*Pointer.BYTES), Pointer.BYTES
        );
    }

    public static Optional<Pointer> getNextPointer(BaseTreeNode treeNode){
        if (treeNode.getData()[treeNode.getData().length - (Pointer.BYTES)] == (byte) 0x0){
            return Optional.empty();
        }

        return Optional.of(
                Pointer.fromByteArray(treeNode.getData(), treeNode.getData().length - (Pointer.BYTES))
        );
    }

    public static void setNextPointer(BaseTreeNode treeNode, Pointer pointer) {
        System.arraycopy(
                pointer.toByteArray(),
                0,
                treeNode.getData(),
                treeNode.getData().length - Pointer.BYTES, Pointer.BYTES
        );
    }

}
