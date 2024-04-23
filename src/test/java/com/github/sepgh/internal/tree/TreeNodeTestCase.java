package com.github.sepgh.internal.tree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Optional;

// Todo: add non-single key tests (refactor from single to multi)
public class TreeNodeTestCase {
    private final byte[] singleKeyInternalNodeRepresentation = {
            0x00, // Not leaf

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
            TreeNode.TYPE_LEAF_NODE, // Not leaf

            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key 1

            // >> Start pointer to child 1
            Pointer.TYPE_DATA,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // position
            0x00, 0x00, 0x00, 0x01, // chunk
            // >> End pointer to child 1

            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,  // Key 2

            // >> Start pointer to child 2
            Pointer.TYPE_DATA,  // type
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,  // position
            0x00, 0x00, 0x00, 0x02, // chunk
            // >> End pointer to child 2
    };


    @Test
    public void testInternalTreeNodeLeaf(){
        TreeNode treeNode = new TreeNode(singleKeyInternalNodeRepresentation);
        Assertions.assertFalse(treeNode.isLeaf());
    }
    @Test
    public void testSingleKeyInternalTreeNodeChildren(){
        TreeNode treeNode = new TreeNode(singleKeyInternalNodeRepresentation);


        Optional<Pointer> optionalChild0Pointer = treeNode.getChildAtIndex(0);
        Assertions.assertTrue(optionalChild0Pointer.isPresent());
        Pointer child0Pointer = optionalChild0Pointer.get();

        Assertions.assertTrue(child0Pointer.isNodePointer());
        Assertions.assertEquals(1, child0Pointer.position());
        Assertions.assertEquals(1, child0Pointer.chunk());


        Optional<Pointer> optionalChild1Pointer = treeNode.getChildAtIndex(1);
        Assertions.assertTrue(optionalChild1Pointer.isPresent());
        Pointer child1Pointer = optionalChild1Pointer.get();

        Assertions.assertTrue(child1Pointer.isNodePointer());
        Assertions.assertEquals(2, child1Pointer.position());
        Assertions.assertEquals(2, child1Pointer.chunk());

        Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> {
            treeNode.getChildAtIndex(2);
        });
    }
    @Test
    public void testSingleKeyInternalTreeNodeChildrenIteration() {
        TreeNode treeNode = new TreeNode(singleKeyInternalNodeRepresentation);

        int i = 0;

        for (Iterator<Pointer> it = treeNode.children(); it.hasNext(); ) {
            Pointer pointer = it.next();
            if (i == 0){
                Assertions.assertTrue(pointer.isNodePointer());
                Assertions.assertEquals(1, pointer.position());
                Assertions.assertEquals(1, pointer.chunk());
            }
            if (i == 1){
                Assertions.assertTrue(pointer.isNodePointer());
                Assertions.assertEquals(2, pointer.position());
                Assertions.assertEquals(2, pointer.chunk());
            }
            i++;
        }
    }


    @Test
    public void testSingleKeyLeafTreeNodeChildren(){
        TreeNode treeNode = new TreeNode(singleKeyLeafNodeRepresentation);

        Optional<Pointer> optionalChild0Pointer = treeNode.getChildAtIndex(0);
        Assertions.assertFalse(optionalChild0Pointer.isPresent());
    }
    @Test
    public void testSingleKeyLeafTreeNodeChildrenIteration(){
        TreeNode treeNode = new TreeNode(singleKeyLeafNodeRepresentation);

        boolean iterated = false;
        for (Iterator<Pointer> it = treeNode.children(); it.hasNext(); ) {
            iterated = true;
            break;
        }

        Assertions.assertFalse(iterated, "Expected the leaf node to not to be able to iterate over children");
    }
}
