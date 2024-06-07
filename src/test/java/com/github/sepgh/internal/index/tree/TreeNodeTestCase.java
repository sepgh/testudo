package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.InternalClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.data.Identifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

public class TreeNodeTestCase {
    private final byte[] singleKeyInternalNodeRepresentation = {
            BaseClusterTreeNode.TYPE_INTERNAL_NODE_BIT, // Not leaf

            // >> Start pointer to child 1
            Pointer.TYPE_NODE,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // position
            0x00, 0x00, 0x00, 0x01, // chunk
            // >> End pointer to child 1

            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key

            // >> Start pointer to child 2
            Pointer.TYPE_NODE,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,  // position
            0x00, 0x00, 0x00, 0x02, // chunk
            // >> End pointer to child 2
    };

    private final byte[] singleKeyLeafNodeRepresentation = {
            BaseClusterTreeNode.TYPE_LEAF_NODE_BIT, // leaf

            0x01,0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key 1

            // >> Start pointer to child 1
            Pointer.TYPE_DATA,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // position
            0x00, 0x00, 0x00, 0x01, // chunk
            // >> End pointer to child 1

            0x01,0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,  // Key 2

            // >> Start pointer to child 2
            Pointer.TYPE_DATA,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,  // position
            0x00, 0x00, 0x00, 0x02, // chunk
            // >> End pointer to child 2
    };

    @Test
    public void testSingleKeyInternalTreeNodeChildren(){
        BaseClusterTreeNode node = BaseClusterTreeNode.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalClusterTreeNode.class, node);
        InternalClusterTreeNode treeNode = (InternalClusterTreeNode) node;

        Pointer child0Pointer = TreeNodeUtils.getChildPointerAtIndex(treeNode, 0, Identifier.BYTES);
        Assertions.assertNotNull(child0Pointer);

        Assertions.assertTrue(child0Pointer.isNodePointer());
        Assertions.assertEquals(1, child0Pointer.getPosition());
        Assertions.assertEquals(1, child0Pointer.getChunk());


        Pointer child1Pointer = TreeNodeUtils.getChildPointerAtIndex(treeNode, 1, Identifier.BYTES);
        Assertions.assertNotNull(child1Pointer);

        Assertions.assertTrue(child1Pointer.isNodePointer());
        Assertions.assertEquals(2, child1Pointer.getPosition());
        Assertions.assertEquals(2, child1Pointer.getChunk());

        Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> TreeNodeUtils.getChildPointerAtIndex(treeNode, 2, Identifier.BYTES));
    }

    @Test
    public void testSingleKeyInternalTreeNodeChildrenIteration() {
        BaseClusterTreeNode node = BaseClusterTreeNode.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalClusterTreeNode.class, node);
        InternalClusterTreeNode treeNode = (InternalClusterTreeNode) node;

        List<InternalClusterTreeNode.ChildPointers> childPointersList = treeNode.getChildPointersList(3);
        Assertions.assertEquals(1, childPointersList.size());

        Assertions.assertTrue(childPointersList.get(0).getLeft().isNodePointer());
        Assertions.assertEquals(1, childPointersList.get(0).getLeft().getPosition());
        Assertions.assertEquals(1, childPointersList.get(0).getLeft().getChunk());

        Assertions.assertTrue(childPointersList.get(0).getRight().isNodePointer());
        Assertions.assertEquals(2, childPointersList.get(0).getRight().getPosition());
        Assertions.assertEquals(2, childPointersList.get(0).getRight().getChunk());
    }

    @Test
    public void testSingleKeyInternalNodeKeysIteration(){
        BaseClusterTreeNode node = BaseClusterTreeNode.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalClusterTreeNode.class, node);
        InternalClusterTreeNode treeNode = (InternalClusterTreeNode) node;

        Iterator<Long> iterator = treeNode.getKeys(3);
        Assertions.assertTrue(iterator.hasNext());

        Long value = iterator.next();
        Assertions.assertEquals(15, value);

        Assertions.assertFalse(iterator.hasNext());

        Assertions.assertEquals(15, treeNode.getKeyList(3).getFirst());
    }

    @Test
    public void testSingleKeyLeafNodeKeysIteration(){
        BaseClusterTreeNode node = BaseClusterTreeNode.fromBytes(singleKeyLeafNodeRepresentation);

        Assertions.assertInstanceOf(LeafClusterTreeNode.class, node);
        LeafClusterTreeNode treeNode = (LeafClusterTreeNode) node;

        Iterator<Long> iterator = treeNode.getKeys(3);

        Assertions.assertTrue(iterator.hasNext());
        Long value = iterator.next();
        Assertions.assertEquals(15, value);

        Assertions.assertTrue(iterator.hasNext());
        value = iterator.next();
        Assertions.assertEquals(16, value);

        Assertions.assertFalse(iterator.hasNext());

        Assertions.assertEquals(15, treeNode.getKeyList(3).get(0));
        Assertions.assertEquals(16, treeNode.getKeyList(3).get(1));
    }

    @Test
    public void testSingleKeyLeafNodeKeyValueIteration(){
        BaseClusterTreeNode node = BaseClusterTreeNode.fromBytes(singleKeyLeafNodeRepresentation);

        Assertions.assertInstanceOf(LeafClusterTreeNode.class, node);
        LeafClusterTreeNode treeNode = (LeafClusterTreeNode) node;

        Iterator<LeafClusterTreeNode.KeyValue> iterator = treeNode.getKeyValues(3);

        Assertions.assertTrue(iterator.hasNext());
        LeafClusterTreeNode.KeyValue next = iterator.next();
        Assertions.assertEquals(15, next.key());
        Pointer pointer = next.value();
        Assertions.assertFalse(pointer.isNodePointer());
        Assertions.assertEquals(1, pointer.getPosition());
        Assertions.assertEquals(1, pointer.getChunk());


        Assertions.assertTrue(iterator.hasNext());
        next = iterator.next();
        Assertions.assertEquals(16, next.key());
        pointer = next.value();
        Assertions.assertFalse(pointer.isNodePointer());
        Assertions.assertEquals(2, pointer.getPosition());
        Assertions.assertEquals(2, pointer.getChunk());

        Assertions.assertFalse(iterator.hasNext());
    }
}
