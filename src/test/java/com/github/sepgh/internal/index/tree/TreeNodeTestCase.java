package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.AbstractTreeNode;
import com.github.sepgh.internal.index.tree.node.InternalTreeNode;
import com.github.sepgh.internal.index.tree.node.NodeFactory;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.data.LongBinaryObjectWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static com.github.sepgh.internal.index.tree.node.AbstractTreeNode.TYPE_INTERNAL_NODE_BIT;
import static com.github.sepgh.internal.index.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;

public class TreeNodeTestCase {
    private final byte[] singleKeyInternalNodeRepresentation = {
            TYPE_INTERNAL_NODE_BIT, // Not leaf

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
            TYPE_LEAF_NODE_BIT, // leaf

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
        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(new LongBinaryObjectWrapper());

        AbstractTreeNode<Long> node = nodeFactory.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalTreeNode.class, node);
        InternalTreeNode<Long> treeNode = (InternalTreeNode<Long>) node;

        Pointer child0Pointer = TreeNodeUtils.getChildPointerAtIndex(treeNode, 0, LongBinaryObjectWrapper.BYTES);
        Assertions.assertNotNull(child0Pointer);

        Assertions.assertTrue(child0Pointer.isNodePointer());
        Assertions.assertEquals(1, child0Pointer.getPosition());
        Assertions.assertEquals(1, child0Pointer.getChunk());


        Pointer child1Pointer = TreeNodeUtils.getChildPointerAtIndex(treeNode, 1, LongBinaryObjectWrapper.BYTES);
        Assertions.assertNotNull(child1Pointer);

        Assertions.assertTrue(child1Pointer.isNodePointer());
        Assertions.assertEquals(2, child1Pointer.getPosition());
        Assertions.assertEquals(2, child1Pointer.getChunk());

        Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> TreeNodeUtils.getChildPointerAtIndex(treeNode, 2, LongBinaryObjectWrapper.BYTES));
    }

    @Test
    public void testSingleKeyInternalTreeNodeChildrenIteration() {
        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(new LongBinaryObjectWrapper());

        AbstractTreeNode<Long> node = nodeFactory.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalTreeNode.class, node);
        InternalTreeNode<Long> treeNode = (InternalTreeNode<Long>) node;

        List<InternalTreeNode.ChildPointers<Long>> childPointersList = treeNode.getChildPointersList(3);
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
        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(new LongBinaryObjectWrapper());
        AbstractTreeNode<Long> node = nodeFactory.fromBytes(singleKeyInternalNodeRepresentation);

        Assertions.assertInstanceOf(InternalTreeNode.class, node);
        InternalTreeNode<Long> treeNode = (InternalTreeNode<Long>) node;

        Iterator<Long> iterator = treeNode.getKeys(3);
        Assertions.assertTrue(iterator.hasNext());

        Long value = iterator.next();
        Assertions.assertEquals(15, value);

        Assertions.assertFalse(iterator.hasNext());

        Assertions.assertEquals(15, treeNode.getKeyList(3).getFirst());
    }

    @Test
    public void testSingleKeyLeafNodeKeysIteration(){
        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(new LongBinaryObjectWrapper());

        AbstractTreeNode<Long> node = nodeFactory.fromBytes(singleKeyLeafNodeRepresentation);

        Assertions.assertInstanceOf(LeafClusterTreeNode.class, node);
        LeafClusterTreeNode<Long> treeNode = (LeafClusterTreeNode<Long>) node;

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
        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(new LongBinaryObjectWrapper());

        AbstractTreeNode<Long> node = nodeFactory.fromBytes(singleKeyLeafNodeRepresentation);

        Assertions.assertInstanceOf(LeafClusterTreeNode.class, node);
        LeafClusterTreeNode<Long> treeNode = (LeafClusterTreeNode<Long>) node;

        Iterator<LeafClusterTreeNode.KeyValue<Long, Pointer>> iterator = treeNode.getKeyValues(3);

        Assertions.assertTrue(iterator.hasNext());
        LeafClusterTreeNode.KeyValue<Long, Pointer> next = iterator.next();
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
