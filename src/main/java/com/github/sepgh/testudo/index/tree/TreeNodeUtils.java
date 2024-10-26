package com.github.sepgh.testudo.index.tree;

import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.utils.BinaryUtils;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;

public class TreeNodeUtils {
    private static final int OFFSET_TREE_NODE_FLAGS_END = 1;
    private static final int OFFSET_INTERNAL_NODE_KEY_BEGIN = OFFSET_TREE_NODE_FLAGS_END + Pointer.BYTES;
    private static final int OFFSET_LEAF_NODE_KEY_BEGIN = OFFSET_TREE_NODE_FLAGS_END;
    private static final int SIZE_LEAF_NODE_SIBLING_POINTERS = 2 * Pointer.BYTES;

    private static int getChildPointerOffset(int index, int keySize) {
        return OFFSET_TREE_NODE_FLAGS_END + (index * (Pointer.BYTES + keySize));
    }

    /**
     *
     * @param treeNode node to read/write from/to
     * @param index to check child pointer
     * @return bool true, if the first byte of the current cursor position is not 0x00
     *         Note that index is the index of the pointer object we want to refer to, not the byte position in byte array
     */
    public static boolean hasChildPointerAtIndex(AbstractTreeNode<?> treeNode, int index, int keySize){
        if (getChildPointerOffset(index, keySize) > treeNode.getData().length)
            return false;

        return treeNode.getData()[getChildPointerOffset(index, keySize)] == Pointer.TYPE_NODE;
    }


    /**
     * @param treeNode node to read/write from/to
     * @param index to check child pointer
     * @return Pointer to child node at index
     */public static Pointer getChildPointerAtIndex(AbstractTreeNode<?> treeNode, int index, int keySize){
        return Pointer.fromBytes(treeNode.getData(), getChildPointerOffset(index, keySize));
    }

    // This will not pull remaining children (won't shift them one step behind)
    public static void removeChildAtIndex(AbstractTreeNode<?> treeNode, int index, int keySize) {
        System.arraycopy(
                new byte[Pointer.BYTES],
                0,
                treeNode.getData(),
                getChildPointerOffset(index, keySize),
                Pointer.BYTES
        );
    }

    /**
     * @param treeNode node to read/write from/to
     * @param index index of the pointer to set
     * @param pointer object to set
     */
    public static void setChildPointerAtIndex(AbstractTreeNode<?> treeNode, int index, Pointer pointer, int keySize){
        if (index == 0){
            System.arraycopy(pointer.toBytes(), 0, treeNode.getData(), OFFSET_TREE_NODE_FLAGS_END, Pointer.BYTES);
        } else {
            System.arraycopy(
                    pointer.toBytes(),
                    0,
                    treeNode.getData(),
                    getChildPointerOffset(index, keySize),
                    Pointer.BYTES
            );
        }
    }

    /**
     * @param treeNode to read/write from/to
     * @param index of the key we are looking for
     * @return the fileOffset where the key is found at
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
    public static <K extends Comparable<K>> boolean hasKeyAtIndex(AbstractTreeNode<?> treeNode, int index, int degree, IndexBinaryObjectFactory<K> kIndexBinaryObjectFactory, int valueSize) {
        if (index >= degree - 1)
            return false;

        int keyStartIndex = getKeyStartOffset(treeNode, index, kIndexBinaryObjectFactory.size(), valueSize);
        if (keyStartIndex + kIndexBinaryObjectFactory.size() > treeNode.getData().length)
            return false;

        return !BinaryUtils.isAllZeros(treeNode.getData(), keyStartIndex, kIndexBinaryObjectFactory.size()) || !BinaryUtils.isAllZeros(treeNode.getData(), keyStartIndex + kIndexBinaryObjectFactory.size(), valueSize);
    }


    public static <E extends Comparable<E>> void setKeyAtIndex(AbstractTreeNode<?> treeNode, int index, IndexBinaryObject<E> indexBinaryObject, int valueSize) {
        int keyStartIndex = getKeyStartOffset(treeNode, index, indexBinaryObject.size(), valueSize);
        System.arraycopy(
                indexBinaryObject.getBytes(),
                0,
                treeNode.getData(),
                keyStartIndex,
                indexBinaryObject.size()
        );
    }

    /**
     * @param treeNode to read/write from/to
     * @param index to read they key at
     * @return key value at index
     */
    public static <K extends Comparable<K>> IndexBinaryObject<K> getKeyAtIndex(AbstractTreeNode<?> treeNode, int index, IndexBinaryObjectFactory<K> kIndexBinaryObjectFactory, int valueSize) {
        int keyStartIndex = getKeyStartOffset(treeNode, index, kIndexBinaryObjectFactory.size(), valueSize);
        return kIndexBinaryObjectFactory.create(treeNode.getData(), keyStartIndex);
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

    public static <K extends Comparable<K>, V> Map.Entry<K, V> getKeyValueAtIndex(
            AbstractTreeNode<K> treeNode,
            int index,
            IndexBinaryObjectFactory<K> kIndexBinaryObjectFactory,
            IndexBinaryObjectFactory<V> vIndexBinaryObjectFactory
    ){
        int keyStartIndex = getKeyStartOffset(treeNode, index, kIndexBinaryObjectFactory.size(), vIndexBinaryObjectFactory.size());
        return new AbstractMap.SimpleImmutableEntry<>(
                kIndexBinaryObjectFactory.create(treeNode.getData(), keyStartIndex).asObject(),
                vIndexBinaryObjectFactory.create(treeNode.getData(), keyStartIndex + kIndexBinaryObjectFactory.size()).asObject()
        );
    }

    public static <K extends Comparable<K>, V> void setKeyValueAtIndex(AbstractTreeNode<?> treeNode, int index, IndexBinaryObject<K> keyInnerObj, IndexBinaryObject<V> valueInnerObj) {
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

    public static <K extends Comparable<K>, V> void addKeyValue(
            AbstractTreeNode<?> treeNode,
            int degree,
            IndexBinaryObjectFactory<K> indexBinaryObjectFactory,
            K key,
            IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory,
            V value,
            int indexToFill
    ) {
        int keySize = indexBinaryObjectFactory.size();
        int valueSize = valueIndexBinaryObjectFactory.size();
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
                indexBinaryObjectFactory.create(key),
                valueIndexBinaryObjectFactory.create(value)
        );

        System.arraycopy(
                temp,
                0,
                treeNode.getData(),
                OFFSET_LEAF_NODE_KEY_BEGIN + ((indexToFill + 1) * (keySize + valueSize)),
                temp.length
        );

    }


    /**
     * Todo: performance improvements may be possible
     *       linear search is used to sort the keys
     *       binary search could be used
     *       {Skipping since not used!}
     */
    public static <K extends Comparable<K>, V> int addKeyValueAndGetIndex(
            AbstractTreeNode<?> treeNode,
            int degree,
            IndexBinaryObjectFactory<K> indexBinaryObjectFactory,
            K key,
            IndexBinaryObjectFactory<V> valueIndexBinaryObjectFactory,
            V value
    ) {
        int indexToFill = -1;
        IndexBinaryObject<K> keyAtIndex;
        int valueSize = valueIndexBinaryObjectFactory.size();

        // Linearly looking for key position
        for (int i = 0; i < degree - 1; i++){
            if (!hasKeyAtIndex(treeNode, i, degree, indexBinaryObjectFactory, valueSize)){
                indexToFill = i;
                break;
            }
            keyAtIndex = getKeyAtIndex(treeNode, i, indexBinaryObjectFactory, valueSize);
            K data = keyAtIndex.asObject();
            if (data.compareTo(key) > 0){
                indexToFill = i;
                break;
            }
        }

        if (indexToFill == -1){
            throw new RuntimeException("Logical chaos! Couldn't find the index to fill ...");
        }

        addKeyValue(treeNode, degree, indexBinaryObjectFactory, key, valueIndexBinaryObjectFactory, value, indexToFill);

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
                Pointer.fromBytes(
                        treeNode.getData(),
                        OFFSET_LEAF_NODE_KEY_BEGIN + ((degree - 1) * (keySize + valueSize))
                )
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

    public static void cleanChildrenPointers(InternalTreeNode<?> treeNode, int degree, int keySize) {
        int len = ((degree - 1) * ((keySize + PointerIndexBinaryObject.BYTES))) + Pointer.BYTES;
        System.arraycopy(
                new byte[len],
                0,
                treeNode.getData(),
                OFFSET_TREE_NODE_FLAGS_END,
                len
        );
    }
}
