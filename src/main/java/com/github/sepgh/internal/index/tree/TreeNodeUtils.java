package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.data.NodeInnerObj;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

    // Todo: this function will shift the remaining space after next child to current child (which we wanted to remove) despite other children existing.
    //       The performance could improve (reduce copy call) by checking if next child exists at all first.
    // Todo: additional to above Todo, currently this function is always called at latest index first, so the whole size check is unnecessary for now
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

    private static <E extends Comparable<E>> Constructor<? extends NodeInnerObj<E>> getNodeInnerObjectConstructor(Class<? extends NodeInnerObj<E>> nodeInnerObjectClass) throws NoSuchMethodException {
        return nodeInnerObjectClass.getConstructor(byte[].class, Integer.TYPE);
    }

    private static <E extends Comparable<E>> Constructor<? extends NodeInnerObj<E>> getNodeInnerObjectConstructorForValue(Class<? extends NodeInnerObj<E>> nodeInnerObjectClass, Class<E> eClass) throws NoSuchMethodException {
        return nodeInnerObjectClass.getConstructor(eClass);
    }

    /**
     * @param treeNode to read/write from/to
     * @param index of the key to check existence
     * @return boolean state of existence of a key in index
     */
    public static <E extends Comparable<E>> boolean hasKeyAtIndex(AbstractTreeNode<?> treeNode, int index, int degree, Class<? extends NodeInnerObj<E>> nodeInnerObjectClass, int keySize, int valueSize) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (index >= degree - 1)
            return false;

        int keyStartIndex = getKeyStartOffset(treeNode, index, keySize, valueSize);
        if (keyStartIndex + keySize > treeNode.getData().length)
            return false;

        Constructor<? extends NodeInnerObj<E>> constructor = getNodeInnerObjectConstructor(nodeInnerObjectClass);
        NodeInnerObj<E> nodeInnerObj = constructor.newInstance(treeNode.getData(), keyStartIndex);
        return nodeInnerObj.exists();
    }


    public static <E extends Comparable<E>> void setKeyAtIndex(AbstractTreeNode<?> treeNode, int index, NodeInnerObj<E> nodeInnerObj, int valueSize) {
        int keyStartIndex = getKeyStartOffset(treeNode, index, nodeInnerObj.size(), valueSize);
        System.arraycopy(
                nodeInnerObj.getBytes(),
                0,
                treeNode.getData(),
                keyStartIndex,
                nodeInnerObj.size()
        );
    }

    /**
     * @param treeNode to read/write from/to
     * @param index to read they key at
     * @return key value at index
     */
    public static <E extends Comparable<E>> NodeInnerObj<E> getKeyAtIndex(AbstractTreeNode<?> treeNode, int index, Class<? extends NodeInnerObj<E>> nodeInnerObjectClass, int keySize, int valueSize) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        int keyStartIndex = getKeyStartOffset(treeNode, index, keySize, valueSize);
        Constructor<? extends NodeInnerObj<E>> nodeInnerObjectConstructor = getNodeInnerObjectConstructor(nodeInnerObjectClass);
        return nodeInnerObjectConstructor.newInstance(treeNode.getData(), keyStartIndex);
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

    public static <K extends Comparable<K>, A extends NodeInnerObj<K>, V extends Comparable<V>, B extends NodeInnerObj<V>> Map.Entry<K, V> getKeyValueAtIndex(
            AbstractTreeNode<K> treeNode,
            int index,
            Class<? extends A> keyInnerObjectClass,
            int keySize,
            Class<? extends B> valueInnerObjectClass,
            int valueSize
    ) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        int keyStartIndex = getKeyStartOffset(treeNode, index, keySize, valueSize);
        return new AbstractMap.SimpleImmutableEntry<>(
                getNodeInnerObjectConstructor(keyInnerObjectClass).newInstance(treeNode.getData(), keyStartIndex).data(),
                getNodeInnerObjectConstructor(valueInnerObjectClass).newInstance(treeNode.getData(), keyStartIndex + keySize).data()
        );
    }

    public static <K extends Comparable<K>, V extends Comparable<V>> void setKeyValueAtIndex(AbstractTreeNode<?> treeNode, int index, NodeInnerObj<K> keyInnerObj, NodeInnerObj<V> valueInnerObj) {
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
     *       alternatively, we can hold a space for metadata which keeps track of the number of keys or values stored
     */
    public static <K extends Comparable<K>, V extends Comparable<V>> int addKeyValueAndGetIndex(
            AbstractTreeNode<?> treeNode,
            int degree,
            NodeInnerObj.Strategy<K> keyStrategy,
            K key,
            int keySize,
            NodeInnerObj.Strategy<V> valueStrategy,
            V value,
            int valueSize
    ) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        int indexToFill = -1;
        NodeInnerObj<K> keyAtIndex;
        for (int i = 0; i < degree - 1; i++){
            keyAtIndex = getKeyAtIndex(treeNode, i, keyStrategy.getNodeInnerObjClass(), keySize, valueSize);
            K data = keyAtIndex.data();
            if (!keyAtIndex.exists() || data.compareTo(key) > 0){
                indexToFill = i;
                break;
            }
        }

        if (indexToFill == -1){
            throw new RuntimeException("F..ed up"); // Todo
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
                keyStrategy.fromObject(key),
                valueStrategy.fromObject(value)
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

    // Todo: this function will shift the remaining space after next KV to current KV (which we wanted to remove) despite other KV existing.
    //       The performance could improve (reduce copy call) by checking if next KV exists at all first.
    public static void removeKeyValueAtIndex(AbstractTreeNode<?> treeNode, int index, int keySize, int valueSize) {
        int nextIndexOffset = getKeyStartOffset(treeNode, index + 1, keySize, valueSize);
        if (nextIndexOffset < treeNode.getData().length - SIZE_LEAF_NODE_SIBLING_POINTERS){
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
