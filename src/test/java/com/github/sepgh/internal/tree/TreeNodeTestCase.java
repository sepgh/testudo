package com.github.sepgh.internal.tree;

import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

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

        Optional<Pointer> optionalChild0Pointer = treeNode.getChildAtIndex(0);
        Assertions.assertTrue(optionalChild0Pointer.isPresent());
        Pointer child0Pointer = optionalChild0Pointer.get();

        Assertions.assertTrue(child0Pointer.isNodePointer());
        Assertions.assertEquals(1, child0Pointer.getPosition());
        Assertions.assertEquals(1, child0Pointer.getChunk());


        Optional<Pointer> optionalChild1Pointer = treeNode.getChildAtIndex(1);
        Assertions.assertTrue(optionalChild1Pointer.isPresent());
        Pointer child1Pointer = optionalChild1Pointer.get();

        Assertions.assertTrue(child1Pointer.isNodePointer());
        Assertions.assertEquals(2, child1Pointer.getPosition());
        Assertions.assertEquals(2, child1Pointer.getChunk());

        Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> treeNode.getChildAtIndex(2));
    }

    @Test
    public void testSingleKeyInternalTreeNodeChildrenIteration() {
        BaseTreeNode node = BaseTreeNode.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalTreeNode.class, node);
        InternalTreeNode treeNode = (InternalTreeNode) node;

        int i = 0;

        for (Iterator<Pointer> it = treeNode.children(); it.hasNext(); ) {
            Pointer pointer = it.next();
            if (i == 0){
                Assertions.assertTrue(pointer.isNodePointer());
                Assertions.assertEquals(1, pointer.getPosition());
                Assertions.assertEquals(1, pointer.getChunk());
            }
            if (i == 1){
                Assertions.assertTrue(pointer.isNodePointer());
                Assertions.assertEquals(2, pointer.getPosition());
                Assertions.assertEquals(2, pointer.getChunk());
            }
            i++;
        }
    }

    @Test
    public void testSingleKeyInternalNodeKeysIteration(){
        BaseTreeNode node = BaseTreeNode.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalTreeNode.class, node);
        InternalTreeNode treeNode = (InternalTreeNode) node;

        Iterator<Long> iterator = treeNode.keys();
        Assertions.assertTrue(iterator.hasNext());

        Long value = iterator.next();
        Assertions.assertEquals(15, value);

        Assertions.assertFalse(iterator.hasNext());

        Assertions.assertEquals(15, treeNode.keyList().getFirst());
    }

    @Test
    public void testSingleKeyLeafNodeKeysIteration(){
        BaseTreeNode node = BaseTreeNode.fromBytes(singleKeyLeafNodeRepresentation);

        Assertions.assertInstanceOf(LeafTreeNode.class, node);
        LeafTreeNode treeNode = (LeafTreeNode) node;

        Iterator<Long> iterator = treeNode.keys();

        Assertions.assertTrue(iterator.hasNext());
        Long value = iterator.next();
        Assertions.assertEquals(15, value);

        Assertions.assertTrue(iterator.hasNext());
        value = iterator.next();
        Assertions.assertEquals(16, value);

        Assertions.assertFalse(iterator.hasNext());

        Assertions.assertEquals(15, treeNode.keyList().get(0));
        Assertions.assertEquals(16, treeNode.keyList().get(1));
    }

    @Test
    public void testSingleKeyLeafNodeKeyValueIteration(){
        BaseTreeNode node = BaseTreeNode.fromBytes(singleKeyLeafNodeRepresentation);

        Assertions.assertInstanceOf(LeafTreeNode.class, node);
        LeafTreeNode treeNode = (LeafTreeNode) node;

        Iterator<Map.Entry<Long, Pointer>> iterator = treeNode.keyValues();

        Assertions.assertTrue(iterator.hasNext());
        Map.Entry<Long, Pointer> next = iterator.next();
        Assertions.assertEquals(15, next.getKey());
        Pointer pointer = next.getValue();
        Assertions.assertFalse(pointer.isNodePointer());
        Assertions.assertEquals(1, pointer.getPosition());
        Assertions.assertEquals(1, pointer.getChunk());


        Assertions.assertTrue(iterator.hasNext());
        next = iterator.next();
        Assertions.assertEquals(16, next.getKey());
        pointer = next.getValue();
        Assertions.assertFalse(pointer.isNodePointer());
        Assertions.assertEquals(2, pointer.getPosition());
        Assertions.assertEquals(2, pointer.getChunk());

        Assertions.assertFalse(iterator.hasNext());
    }
}
