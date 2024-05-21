package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

public class TreeNodeTestCase {
    private final byte[] singleKeyInternalNodeRepresentation = {
            BaseTreeNode.TYPE_INTERNAL_NODE_BIT, // Not leaf

            // >> Start pointer to child 1
            Pointer.TYPE_NODE,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // position
            0x00, 0x00, 0x00, 0x01, // chunk
            // >> End pointer to child 1

            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key

            // >> Start pointer to child 2
            Pointer.TYPE_NODE,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,  // position
            0x00, 0x00, 0x00, 0x02, // chunk
            // >> End pointer to child 2
    };

    private final byte[] singleKeyLeafNodeRepresentation = {
            BaseTreeNode.TYPE_LEAF_NODE_BIT, // leaf

            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key 1

            // >> Start pointer to child 1
            Pointer.TYPE_DATA,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // position
            0x00, 0x00, 0x00, 0x01, // chunk
            // >> End pointer to child 1

            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,  // Key 2

            // >> Start pointer to child 2
            Pointer.TYPE_DATA,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,  // position
            0x00, 0x00, 0x00, 0x02, // chunk
            // >> End pointer to child 2
    };

    @Test
    public void testSingleKeyInternalTreeNodeChildren(){
        BaseTreeNode node = BaseTreeNode.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalTreeNode.class, node);
        InternalTreeNode treeNode = (InternalTreeNode) node;

        Pointer child0Pointer = TreeNodeUtils.getChildPointerAtIndex(treeNode, 0);
        Assertions.assertNotNull(child0Pointer);

        Assertions.assertTrue(child0Pointer.isNodePointer());
        Assertions.assertEquals(1, child0Pointer.getPosition());
        Assertions.assertEquals(1, child0Pointer.getChunk());


        Pointer child1Pointer = TreeNodeUtils.getChildPointerAtIndex(treeNode, 1);
        Assertions.assertNotNull(child1Pointer);

        Assertions.assertTrue(child1Pointer.isNodePointer());
        Assertions.assertEquals(2, child1Pointer.getPosition());
        Assertions.assertEquals(2, child1Pointer.getChunk());

        Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> TreeNodeUtils.getChildPointerAtIndex(treeNode, 2));
    }

    @Test
    public void testSingleKeyInternalTreeNodeChildrenIteration() {
        BaseTreeNode node = BaseTreeNode.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalTreeNode.class, node);
        InternalTreeNode treeNode = (InternalTreeNode) node;

        List<InternalTreeNode.ChildPointers> childPointersList = treeNode.getChildPointersList(3);
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
        BaseTreeNode node = BaseTreeNode.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalTreeNode.class, node);
        InternalTreeNode treeNode = (InternalTreeNode) node;

        Iterator<Long> iterator = treeNode.getKeys(3);
        Assertions.assertTrue(iterator.hasNext());

        Long value = iterator.next();
        Assertions.assertEquals(15, value);

        Assertions.assertFalse(iterator.hasNext());

        Assertions.assertEquals(15, treeNode.getKeyList(3).getFirst());
    }

    @Test
    public void testSingleKeyLeafNodeKeysIteration(){
        BaseTreeNode node = BaseTreeNode.fromBytes(singleKeyLeafNodeRepresentation);

        Assertions.assertInstanceOf(LeafTreeNode.class, node);
        LeafTreeNode treeNode = (LeafTreeNode) node;

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
        BaseTreeNode node = BaseTreeNode.fromBytes(singleKeyLeafNodeRepresentation);

        Assertions.assertInstanceOf(LeafTreeNode.class, node);
        LeafTreeNode treeNode = (LeafTreeNode) node;

        Iterator<LeafTreeNode.KeyValue> iterator = treeNode.getKeyValues(3);

        Assertions.assertTrue(iterator.hasNext());
        LeafTreeNode.KeyValue next = iterator.next();
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
