package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.data.ImmutableBinaryObjectWrapper;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;

public class TreeNodeUtils {
    private static final int OFFSET_TREE_NODE_FLAGS_END = 1;
    private static final int OFFSET_INTERNAL_NODE_KEY_BEGIN = OFFSET_TREE_NODE_FLAGS_END + Pointer.BYTES;
    private static final int OFFSET_LEAF_NODE_KEY_BEGIN = OFFSET_TREE_NODE_FLAGS_END;
    private static final int SIZE_LEAF_NODE_SIBLING_POINTERS = 2 * Pointer.BYTES;

    /**
     *
     * @param treeNode node to read/write from/to
     * @param index to check child pointer
     * @return bool true, if the first byte of the current cursor position is not 0x00
     *         Note that index is the index of the pointer object we want to refer to, not the byte position in byte array
     */
    public static boolean hasChildPointerAtIndex(AbstractTreeNode<?> treeNode, int index, int keySize){
        if (OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + keySize)) > treeNode.getData().length)
            return false;


        return treeNode.getData()[OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + keySize))] == Pointer.TYPE_NODE;
    }


    /**
     * @param treeNode node to read/write from/to
     * @param index to check child pointer
     * @return Pointer to child node at index
     */public static Pointer getChildPointerAtIndex(AbstractTreeNode<?> treeNode, int index, int keySize){
        return Pointer.fromBytes(treeNode.getData(), OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + keySize)));
    }

    // This will not pull remaining children (won't shift them one step behind)
    public static void removeChildAtIndex(AbstractTreeNode<?> treeNode, int index, int keySize) {
        System.arraycopy(
                new byte[Pointer.BYTES],
                0,
                treeNode.getData(),
                OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + keySize)),
                Pointer.BYTES
        );
    }

    /**
     * @param treeNode node to read/write from/to
     * @param index index of the pointer to set
     * @param pointer object to set
     */
    public static void setPointerToChild(AbstractTreeNode<?> treeNode, int index, Pointer pointer, int keySize){
        if (index == 0){
            System.arraycopy(pointer.toBytes(), 0, treeNode.getData(), OFFSET_TREE_NODE_FLAGS_END, Pointer.BYTES);
        } else {
            System.arraycopy(
                    pointer.toBytes(),
                    0,
                    treeNode.getData(),
                    OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + keySize)),
                    Pointer.BYTES
            );
        }
    }

    /**
     * @param treeNode to read/write from/to
     * @param index of the key we are looking for
     * @return the offset where the key is found at
     */
    private static int getKeyStartOffset(AbstractTreeNode<?> treeNode, int index, int keySize, int valueSize) {
        if (!treeNode.isLeaf()){
            return OFFSET_INTERNAL_NODE_KEY_BEGIN + (index * (keySize + valueSize));
        } else {
            return OFFSET_TREE_NODE_FLAGS_END + (index * (keySize + valueSize));
        }
    }

    /**
     * @param treeNode to read/write from/to
     * @param index of the key to check existence
     * @return boolean state of existence of a key in index
     */
    public static <K extends Comparable<K>> boolean hasKeyAtIndex(AbstractTreeNode<?> treeNode, int index, int degree, ImmutableBinaryObjectWrapper<K> kImmutableBinaryObjectWrapper, int valueSize) {
        if (index >= degree - 1)
            return false;

        int keyStartIndex = getKeyStartOffset(treeNode, index, kImmutableBinaryObjectWrapper.size(), valueSize);
        if (keyStartIndex + kImmutableBinaryObjectWrapper.size() > treeNode.getData().length)
            return false;
        return kImmutableBinaryObjectWrapper.load(treeNode.getData(), keyStartIndex).hasValue();
    }


    public static <E extends Comparable<E>> void setKeyAtIndex(AbstractTreeNode<?> treeNode, int index, ImmutableBinaryObjectWrapper<E> immutableBinaryObjectWrapper, int valueSize) {
        int keyStartIndex = getKeyStartOffset(treeNode, index, immutableBinaryObjectWrapper.size(), valueSize);
        System.arraycopy(
                immutableBinaryObjectWrapper.getBytes(),
                0,
                treeNode.getData(),
                keyStartIndex,
                immutableBinaryObjectWrapper.size()
        );
    }

    /**
     * @param treeNode to read/write from/to
     * @param index to read they key at
     * @return key value at index
     */
    public static <K extends Comparable<K>> ImmutableBinaryObjectWrapper<K> getKeyAtIndex(AbstractTreeNode<?> treeNode, int index, ImmutableBinaryObjectWrapper<K> kImmutableBinaryObjectWrapper, int valueSize) {
        int keyStartIndex = getKeyStartOffset(treeNode, index, kImmutableBinaryObjectWrapper.size(), valueSize);
        return kImmutableBinaryObjectWrapper.load(treeNode.getData(), keyStartIndex);
    }

    public static void removeKeyAtIndex(AbstractTreeNode<?> treeNode, int index, int keySize, int valueSize) {
        System.arraycopy(
                new byte[keySize],
                0,
                treeNode.getData(),
                getKeyStartOffset(treeNode, index, keySize, valueSize),
                keySize
        );
    }

    public static <K extends Comparable<K>, V extends Comparable<V>> Map.Entry<K, V> getKeyValueAtIndex(
            AbstractTreeNode<K> treeNode,
            int index,
            ImmutableBinaryObjectWrapper<K> kImmutableBinaryObjectWrapper,
            ImmutableBinaryObjectWrapper<V> vImmutableBinaryObjectWrapper
    ){
        int keyStartIndex = getKeyStartOffset(treeNode, index, kImmutableBinaryObjectWrapper.size(), vImmutableBinaryObjectWrapper.size());
        return new AbstractMap.SimpleImmutableEntry<K, V>(
                kImmutableBinaryObjectWrapper.load(treeNode.getData(), keyStartIndex).asObject(),
                vImmutableBinaryObjectWrapper.load(treeNode.getData(), keyStartIndex + kImmutableBinaryObjectWrapper.size()).asObject()
        );
    }

    public static <K extends Comparable<K>, V extends Comparable<V>> void setKeyValueAtIndex(AbstractTreeNode<?> treeNode, int index, ImmutableBinaryObjectWrapper<K> keyInnerObj, ImmutableBinaryObjectWrapper<V> valueInnerObj) {
        System.arraycopy(
                keyInnerObj.getBytes(),
                0,
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + (index * (keyInnerObj.size() + valueInnerObj.size())),
                keyInnerObj.size()
        );


        System.arraycopy(
                valueInnerObj.getBytes(),
                0,
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + (index * (keyInnerObj.size() + valueInnerObj.size())) + keyInnerObj.size(),
                valueInnerObj.size()
        );

    }


    /**
     * Todo: performance improvements may be possible
     *       linear search is used to sort the keys
     *       binary search could be used
     */
    public static <K extends Comparable<K>, V extends Comparable<V>> int addKeyValueAndGetIndex(
            AbstractTreeNode<?> treeNode,
            int degree,
            ImmutableBinaryObjectWrapper<K> immutableBinaryObjectWrapper,
            K key,
            int keySize,
            ImmutableBinaryObjectWrapper<V> valueImmutableBinaryObjectWrapper,
            V value,
            int valueSize
    ) throws ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        int indexToFill = -1;
        ImmutableBinaryObjectWrapper<K> keyAtIndex;

        // Linearly looking for key position
        for (int i = 0; i < degree - 1; i++){
            keyAtIndex = getKeyAtIndex(treeNode, i, immutableBinaryObjectWrapper, valueSize);
            K data = keyAtIndex.asObject();
            if (!keyAtIndex.hasValue() || data.compareTo(key) > 0){
                indexToFill = i;
                break;
            }
        }

        if (indexToFill == -1){
            throw new RuntimeException("Logical chaos! Couldn't find the index to fill ...");
        }


        int max = degree - 1;
        int bufferSize = ((max - indexToFill - 1) * (keySize + valueSize));

        byte[] temp = new byte[bufferSize];
        System.arraycopy(
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + (indexToFill * (keySize + valueSize)),
                temp,
                0,
                temp.length
        );

        setKeyValueAtIndex(
                treeNode,
                indexToFill,
                immutableBinaryObjectWrapper.load(key),
                valueImmutableBinaryObjectWrapper.load(value)
        );

        System.arraycopy(
                temp,
                0,
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + ((indexToFill + 1) * (keySize + valueSize)),
                temp.length
        );

        return indexToFill;

    }

    public static void removeKeyValueAtIndex(AbstractTreeNode<?> treeNode, int index, int keySize, int valueSize) {
        int nextIndexOffset = getKeyStartOffset(treeNode, index + 1, keySize, valueSize);
        if (nextIndexOffset < treeNode.getData().length - SIZE_LEAF_NODE_SIBLING_POINTERS){
            // Last key value (in terms of position in node, not number of filled kv) is being removed. No shifting required
            System.arraycopy(
                    new byte[keySize + valueSize],
                    0,
                    treeNode.getData(),
                    getKeyStartOffset(treeNode, index, keySize, valueSize),
                    keySize + valueSize
            );
        } else {
            System.arraycopy(
                    treeNode.getData(),
                    nextIndexOffset,
                    treeNode.getData(),
                    getKeyStartOffset(treeNode, index, keySize, valueSize),
                    nextIndexOffset - SIZE_LEAF_NODE_SIBLING_POINTERS
            );
            System.arraycopy(
                    new byte[keySize + valueSize],
                    0,
                    treeNode.getData(),
                    treeNode.getData().length - SIZE_LEAF_NODE_SIBLING_POINTERS - (keySize + valueSize),
                    keySize + valueSize
            );
        }
    }

    public static Optional<Pointer> getPreviousPointer(AbstractTreeNode<?> treeNode, int degree, int keySize, int valueSize){
        if (treeNode.getData()[OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize))] == (byte) 0x0){
            return Optional.empty();
        }

        return Optional.of(
                Pointer.fromBytes(treeNode.getData(), OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize)))
        );
    }

    public static void setPreviousPointer(AbstractTreeNode<?> treeNode, int degree, Pointer pointer, int keySize, int valueSize) {
        System.arraycopy(
                pointer.toBytes(),
                0,
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize)),
                Pointer.BYTES
        );
    }

    public static Optional<Pointer> getNextPointer(AbstractTreeNode<?> treeNode, int degree, int keySize, int valueSize) {
        if (treeNode.getData()[OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize)) + Pointer.BYTES] == (byte) 0x0){
            return Optional.empty();
        }

        return Optional.of(
                Pointer.fromBytes(treeNode.getData(), OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize)) + Pointer.BYTES)
        );
    }

    public static void setNextPointer(AbstractTreeNode<?> treeNode, int degree, Pointer pointer, int keySize, int valueSize) {
        System.arraycopy(
                pointer.toBytes(),
                0,
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize)) + Pointer.BYTES,
                Pointer.BYTES
        );
    }

    public static void cleanChildrenPointers(InternalTreeNode<?> treeNode, int degree, int keySize, int valueSize) {
        int len = ((degree - 1) * ((keySize + valueSize)) + Pointer.BYTES);
        System.arraycopy(
                new byte[len],
                0,
                treeNode.getData(),
                OFFSET_TREE_NODE_FLAGS_END,
                len
        );
    }
}
