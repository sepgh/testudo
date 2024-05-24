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
    private static final int SIZE_LEAF_NODE_SIBLING_POINTERS = 2 * Pointer.BYTES;

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
        return Pointer.fromBytes(treeNode.getData(), OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + Long.BYTES)));
    }

    // Todo: this function will shift the remaining space after next child to current child (which we wanted to remove) despite other children existing.
    //       The performance could improve (reduce copy call) by checking if next child exists at all first.
    // Todo: additional to above Todo, currently this function is always called at latest index first, so the whole size check is unnecessary for now
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
            System.arraycopy(pointer.toBytes(), 0, treeNode.getData(), OFFSET_TREE_NODE_FLAGS_END, Pointer.BYTES);
        } else {
            System.arraycopy(
                    pointer.toBytes(),
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
    public static boolean hasKeyAtIndex(BaseTreeNode treeNode, int index, int degree){
        if (index >= degree - 1)
            return false;

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
            Pointer.fromBytes(treeNode.getData(), keyStartIndex + Long.BYTES)
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

        pointer.fillBytes(
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
    public static int addKeyValueAndGetIndex(BaseTreeNode treeNode, long key, Pointer pointer, int degree) {
        int indexToFill = -1;
        long keyAtIndex;
        for (int i = 0; i < degree - 1; i++){
            keyAtIndex = getKeyAtIndex(treeNode, i);
            if (keyAtIndex == 0 || keyAtIndex > key){
                indexToFill = i;
                break;
            }
        }

        if (indexToFill == -1){
            throw new RuntimeException("F..ed up"); // Todo
        }


        int max = degree - 1;
        int bufferSize = ((max - indexToFill - 1) * SIZE_LEAF_NODE_KEY_POINTER);

        byte[] temp = new byte[bufferSize];
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
                OFFSET_LEAF_NODE_KEY_BEGIN + ((indexToFill + 1) * (SIZE_LEAF_NODE_KEY_POINTER)),
                temp.length
        );


        return indexToFill;

    }

    // Todo: this function will shift the remaining space after next KV to current KV (which we wanted to remove) despite other KV existing.
    //       The performance could improve (reduce copy call) by checking if next KV exists at all first.
    public static void removeKeyValueAtIndex(BaseTreeNode treeNode, int index) {
        int nextIndexOffset = getKeyStartOffset(treeNode, index + 1);
        if (nextIndexOffset < treeNode.getData().length - SIZE_LEAF_NODE_SIBLING_POINTERS){
            System.arraycopy(
                    new byte[Long.BYTES + Pointer.BYTES],
                    0,
                    treeNode.getData(),
                    getKeyStartOffset(treeNode, index),
                    Long.BYTES + Pointer.BYTES
            );
        } else {
            System.arraycopy(
                    treeNode.getData(),
                    nextIndexOffset,
                    treeNode.getData(),
                    getKeyStartOffset(treeNode, index),
                    nextIndexOffset - SIZE_LEAF_NODE_SIBLING_POINTERS
            );
            System.arraycopy(
                    new byte[SIZE_LEAF_NODE_KEY_POINTER],
                    0,
                    treeNode.getData(),
                    treeNode.getData().length - SIZE_LEAF_NODE_SIBLING_POINTERS - SIZE_LEAF_NODE_KEY_POINTER,
                    SIZE_LEAF_NODE_KEY_POINTER
            );
        }
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
    public static int addKeyAndGetIndex(BaseTreeNode treeNode, long key, int degree) {
        // Shall only be called on internal nodes
        if (treeNode.isLeaf()){
            throw new RuntimeException();  // Todo: not runtime
        }

        int indexToFill = 0;
        long keyAtIndex = 0;
        for (int i = 0; i < degree - 1; i++){
            keyAtIndex = getKeyAtIndex(treeNode, i);
            if (keyAtIndex == 0 || keyAtIndex > key){
                indexToFill = i;
                break;
            }
        }

        if (keyAtIndex == 0){
            System.arraycopy(
                    Longs.toByteArray(key),
                    0,
                    treeNode.getData(),
                    OFFSET_INTERNAL_NODE_KEY_BEGIN + (indexToFill * (SIZE_INTERNAL_NODE_KEY_POINTER)),
                    Long.BYTES
            );
            return indexToFill;
        }


        int max = degree - 1;
        int fullSize = (max * SIZE_INTERNAL_NODE_KEY_POINTER) + OFFSET_INTERNAL_NODE_KEY_BEGIN;
        int bufferSize = fullSize - ((indexToFill + 1) * SIZE_INTERNAL_NODE_KEY_POINTER) - OFFSET_INTERNAL_NODE_KEY_BEGIN;

        byte[] temp = new byte[bufferSize];
        System.arraycopy(
                treeNode.getData(),
                OFFSET_INTERNAL_NODE_KEY_BEGIN + (indexToFill * SIZE_INTERNAL_NODE_KEY_POINTER),
                temp,
                0,
                temp.length
        );

        System.arraycopy(
                Longs.toByteArray(key),
                0,
                treeNode.getData(),
                OFFSET_INTERNAL_NODE_KEY_BEGIN + (indexToFill * SIZE_INTERNAL_NODE_KEY_POINTER),
                Long.BYTES
        );

        System.arraycopy(
                temp,
                0,
                treeNode.getData(),
                OFFSET_INTERNAL_NODE_KEY_BEGIN + ((indexToFill + 1) * SIZE_INTERNAL_NODE_KEY_POINTER),
                temp.length
        );

        return indexToFill;
    }

    public static Optional<Pointer> getPreviousPointer(BaseTreeNode treeNode, int degree){
        if (treeNode.getData()[OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * SIZE_LEAF_NODE_KEY_POINTER)] == (byte) 0x0){
            return Optional.empty();
        }

        return Optional.of(
                Pointer.fromBytes(treeNode.getData(), OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * SIZE_LEAF_NODE_KEY_POINTER))
        );
    }

    public static void setPreviousPointer(BaseTreeNode treeNode, int degree, Pointer pointer) {
        System.arraycopy(
                pointer.toBytes(),
                0,
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * SIZE_LEAF_NODE_KEY_POINTER),
                Pointer.BYTES
        );
    }

    public static Optional<Pointer> getNextPointer(BaseTreeNode treeNode, int degree) {
        if (treeNode.getData()[OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * SIZE_LEAF_NODE_KEY_POINTER) + Pointer.BYTES] == (byte) 0x0){
            return Optional.empty();
        }

        return Optional.of(
                Pointer.fromBytes(treeNode.getData(), OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * SIZE_LEAF_NODE_KEY_POINTER) + Pointer.BYTES)
        );
    }

    public static void setNextPointer(BaseTreeNode treeNode, int degree, Pointer pointer) {
        System.arraycopy(
                pointer.toBytes(),
                0,
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * SIZE_LEAF_NODE_KEY_POINTER) + Pointer.BYTES,
                Pointer.BYTES
        );
    }

    public static void cleanChildrenPointers(InternalTreeNode treeNode, int degree) {
        int len = ((degree - 1) * (SIZE_INTERNAL_NODE_KEY_POINTER) + Pointer.BYTES);
        System.arraycopy(
                new byte[len],
                0,
                treeNode.getData(),
                OFFSET_TREE_NODE_FLAGS_END,
                len
        );
    }
}
