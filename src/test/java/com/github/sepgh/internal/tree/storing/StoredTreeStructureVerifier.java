package com.github.sepgh.internal.tree.storing;

import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.TreeNodeIO;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;
import com.google.common.hash.HashCode;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class StoredTreeStructureVerifier {


    /*
     * 009
     * ├── .
     * │   ├── 001
     * │   └── 002
     * ├── 003
     * │   ├── 003
     * │   ├── 004
     * │   └── 005
     * ├── 006
     * │   ├── 006
     * │   ├── 007
     * │   └── 008
     * ├── .
     * │   ├── 009
     * │   └── 010
     * └── 011
     *     ├── 011
     *     └── 012
     */
    public static void testUnOrderedTreeStructure1(IndexStorageManager indexStorageManager, int table, long multi, int degree) throws ExecutionException, InterruptedException {
        Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(table).get();
        Assertions.assertTrue(optional.isPresent());

        BaseTreeNode rootNode = BaseTreeNode.fromBytes(optional.get().bytes());
        Assertions.assertTrue(rootNode.isRoot());
        Assertions.assertFalse(rootNode.isLeaf());

        Assertions.assertEquals(multi * 9, rootNode.getKeys(degree).next());

        // Checking root child at left
        InternalTreeNode leftChildInternalNode = (InternalTreeNode) TreeNodeIO.read(indexStorageManager, table, ((InternalTreeNode) rootNode).getKeyPointersList(degree).get(0).getLeft());
        List<Long> leftChildInternalNodeKeys = leftChildInternalNode.getKeyList(degree);
        Assertions.assertEquals(2, leftChildInternalNodeKeys.size());
        Assertions.assertEquals(multi * 3, leftChildInternalNodeKeys.get(0));
        Assertions.assertEquals(multi * 6, leftChildInternalNodeKeys.get(1));

        // Far left leaf
        LeafTreeNode currentLeaf = (LeafTreeNode) TreeNodeIO.read(indexStorageManager, table, leftChildInternalNode.getKeyPointersList(degree).get(0).getLeft());
        List<Long> currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 1, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 2, currentLeafKeys.get(1));

        // 2nd Leaf
        Optional<Pointer> nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), leftChildInternalNode.getKeyPointersList(degree).get(0).getRight());

        currentLeaf = (LeafTreeNode) TreeNodeIO.read(indexStorageManager, table, leftChildInternalNode.getKeyPointersList(degree).get(0).getRight());
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(3, currentLeafKeys.size());
        Assertions.assertEquals(multi * 3, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 4, currentLeafKeys.get(1));
        Assertions.assertEquals(multi * 5, currentLeafKeys.get(2));

        //3rd leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);

        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), leftChildInternalNode.getKeyPointersList(degree).get(1).getRight());

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        leftChildInternalNode.getKeyPointersList(degree).get(1).getRight()
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(3, currentLeafKeys.size());
        Assertions.assertEquals(multi * 6, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 7, currentLeafKeys.get(1));
        Assertions.assertEquals(multi * 8, currentLeafKeys.get(2));


        // Checking root child at right
        InternalTreeNode rightChildInternalNode = (InternalTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        ((InternalTreeNode) rootNode).getKeyPointersList(degree).get(0).getRight()).get().bytes()
        );
        List<Long> rightChildInternalNodeKeys = rightChildInternalNode.getKeyList(degree);
        Assertions.assertEquals(1, rightChildInternalNodeKeys.size());
        Assertions.assertEquals(multi * 11, rightChildInternalNodeKeys.get(0));

        // 4th leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNode.getKeyPointersList(degree).get(0).getLeft());

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        rightChildInternalNode.getKeyPointersList(degree).get(0).getLeft()
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 9, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 10, currentLeafKeys.get(1));


        // 5th leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNode.getKeyPointersList(degree).get(0).getRight());

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        rightChildInternalNode.getKeyPointersList(degree).get(0).getRight()
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 11, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 12, currentLeafKeys.get(1));
    }


    /*
     * Verifies the shape of the tree will be like below
     * 007
     * ├── .
     * │   ├── 001
     * │   └── 002
     * ├── 003
     * │   ├── 003
     * │   └── 004
     * ├── 005
     * │   ├── 005
     * │   └── 006
     * ├── .
     * │   ├── 007
     * │   └── 008
     * ├── 009
     * │   ├── 009
     * │   └── 010
     * └── 0011
     *     ├── 011
     *     └── 012
     */
    public static void testOrderedTreeStructure(IndexStorageManager indexStorageManager, int table, long multi, int degree) throws ExecutionException, InterruptedException {
        Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(table).get();
        Assertions.assertTrue(optional.isPresent());

        BaseTreeNode rootNode = BaseTreeNode.fromBytes(optional.get().bytes());
        Assertions.assertTrue(rootNode.isRoot());
        Assertions.assertFalse(rootNode.isLeaf());

        Assertions.assertEquals(multi*7, rootNode.getKeys(degree).next());

        // Checking root child at left
        InternalTreeNode leftChildInternalNode = (InternalTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        ((InternalTreeNode) rootNode).getKeyPointersList(degree).get(0).getLeft()).get().bytes()
        );
        List<Long> leftChildInternalNodeKeys = leftChildInternalNode.getKeyList(degree);
        Assertions.assertEquals(2, leftChildInternalNodeKeys.size(), ""+leftChildInternalNodeKeys);
        Assertions.assertEquals(multi * 3, leftChildInternalNodeKeys.get(0));
        Assertions.assertEquals(multi * 5, leftChildInternalNodeKeys.get(1));

        // Far left leaf
        LeafTreeNode currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        leftChildInternalNode.getKeyPointersList(degree).get(0).getLeft()
                ).get().bytes()
        );
        List<Long> currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 1, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 2, currentLeafKeys.get(1));

        // 2nd Leaf
        Optional<Pointer> nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), leftChildInternalNode.getKeyPointersList(degree).get(0).getRight());

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        leftChildInternalNode.getKeyPointersList(degree).get(0).getRight()
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 3, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 4, currentLeafKeys.get(1));

        //3rd leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), leftChildInternalNode.getKeyPointersList(degree).get(1).getRight());

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        leftChildInternalNode.getKeyPointersList(degree).get(1).getRight()
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 5, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 6, currentLeafKeys.get(1));


        // Checking root child at right
        InternalTreeNode rightChildInternalNode = (InternalTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        ((InternalTreeNode) rootNode).getKeyPointersList(degree).get(0).getRight()).get().bytes()
        );
        List<Long> rightChildInternalNodeKeys = rightChildInternalNode.getKeyList(degree);
        Assertions.assertEquals(2, rightChildInternalNodeKeys.size());
        Assertions.assertEquals(multi * 9, rightChildInternalNodeKeys.get(0));
        Assertions.assertEquals(multi * 11, rightChildInternalNodeKeys.get(1));

        // 4th leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNode.getKeyPointersList(degree).get(0).getLeft());

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        rightChildInternalNode.getKeyPointersList(degree).get(0).getLeft()
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 7, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 8, currentLeafKeys.get(1));


        // 5th leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNode.getKeyPointersList(degree).get(0).getRight());

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        rightChildInternalNode.getKeyPointersList(degree).get(0).getRight()
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 9, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 10, currentLeafKeys.get(1));


        // 6th node
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNode.getKeyPointersList(degree).get(1).getRight());

        currentLeaf = (LeafTreeNode) BaseTreeNode.fromBytes(
                indexStorageManager.readNode(
                        table,
                        rightChildInternalNode.getKeyPointersList(degree).get(1).getRight()
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 11, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 12, currentLeafKeys.get(1));
    }

}
