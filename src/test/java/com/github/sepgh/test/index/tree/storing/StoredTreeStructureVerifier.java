package com.github.sepgh.test.index.tree.storing;

import com.github.sepgh.testudo.ds.KVSize;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexTreeNodeIO;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;
import static com.github.sepgh.test.TestParams.DEFAULT_KV_SIZE;

public class StoredTreeStructureVerifier {
    private final static KVSize KV_SIZE = new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES);

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
    public static void testUnOrderedTreeStructure1(IndexStorageManager indexStorageManager, int table, long multi, int degree) throws ExecutionException, InterruptedException, IOException, InternalOperationException {
        Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(table, KV_SIZE).get();
        Assertions.assertTrue(optional.isPresent());
        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        AbstractTreeNode<Long> rootNode = nodeFactory.fromBytes(optional.get().bytes());
        Assertions.assertTrue(rootNode.isRoot());
        Assertions.assertFalse(rootNode.isLeaf());

        Assertions.assertEquals(multi * 9, rootNode.getKeys(degree, PointerIndexBinaryObject.BYTES).next());

        // Checking root child at left
        InternalTreeNode<Long> leftChildInternalNode = (InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, table, ((InternalTreeNode<Long>) rootNode).getChildPointersList(degree).get(0).getLeft(), nodeFactory, DEFAULT_KV_SIZE);
        List<Long> leftChildInternalNodeKeys = leftChildInternalNode.getKeyList(degree);
        Assertions.assertEquals(2, leftChildInternalNodeKeys.size());
        Assertions.assertEquals(multi * 3, leftChildInternalNodeKeys.get(0));
        Assertions.assertEquals(multi * 6, leftChildInternalNodeKeys.get(1));

        // Far left leaf
        LeafClusterTreeNode<Long> currentLeaf = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, table, leftChildInternalNode.getChildPointersList(degree).get(0).getLeft(), nodeFactory, DEFAULT_KV_SIZE);
        List<Long> currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 1, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 2, currentLeafKeys.get(1));

        // 2nd Leaf
        Optional<Pointer> nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), leftChildInternalNode.getChildAtIndex(1));

        currentLeaf = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, table, leftChildInternalNode.getChildPointersList(degree).get(0).getRight(), nodeFactory, DEFAULT_KV_SIZE);
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(3, currentLeafKeys.size());
        Assertions.assertEquals(multi * 3, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 4, currentLeafKeys.get(1));
        Assertions.assertEquals(multi * 5, currentLeafKeys.get(2));

        //3rd leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);

        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), leftChildInternalNode.getChildPointersList(degree).get(1).getRight());

        currentLeaf = (LeafClusterTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        leftChildInternalNode.getChildPointersList(degree).get(1).getRight(),
                        KV_SIZE
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(3, currentLeafKeys.size());
        Assertions.assertEquals(multi * 6, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 7, currentLeafKeys.get(1));
        Assertions.assertEquals(multi * 8, currentLeafKeys.get(2));


        // Checking root child at right
        InternalTreeNode<Long> rightChildInternalNode = (InternalTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        ((InternalTreeNode<Long>) rootNode).getChildPointersList(degree).get(0).getRight(),
                        KV_SIZE
                ).get().bytes()
        );
        List<Long> rightChildInternalNodeKeys = rightChildInternalNode.getKeyList(degree);
        Assertions.assertEquals(1, rightChildInternalNodeKeys.size());
        Assertions.assertEquals(multi * 11, rightChildInternalNodeKeys.get(0));

        // 4th leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNode.getChildPointersList(degree).get(0).getLeft());

        currentLeaf = (LeafClusterTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        rightChildInternalNode.getChildPointersList(degree).get(0).getLeft(),
                        KV_SIZE
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 9, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 10, currentLeafKeys.get(1));


        // 5th leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNode.getChildPointersList(degree).get(0).getRight());

        currentLeaf = (LeafClusterTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        rightChildInternalNode.getChildPointersList(degree).get(0).getRight(),
                        KV_SIZE
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
    public static void testOrderedTreeStructure(IndexStorageManager indexStorageManager, int table, long multi, int degree) throws ExecutionException, InterruptedException, IOException, InternalOperationException {
        Optional<IndexStorageManager.NodeData> optional = indexStorageManager.getRoot(table, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get();
        Assertions.assertTrue(optional.isPresent());
        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        AbstractTreeNode<Long> rootNode = nodeFactory.fromBytes(optional.get().bytes());
        Assertions.assertTrue(rootNode.isRoot());
        Assertions.assertFalse(rootNode.isLeaf());

        Assertions.assertEquals(multi*7, rootNode.getKeys(degree, PointerIndexBinaryObject.BYTES).next());

        // Checking root child at left
        InternalTreeNode<Long> leftChildInternalNode = (InternalTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        ((InternalTreeNode<Long>) rootNode).getChildPointersList(degree).get(0).getLeft(), KV_SIZE).get().bytes()
        );
        List<Long> leftChildInternalNodeKeys = leftChildInternalNode.getKeyList(degree);
        Assertions.assertEquals(2, leftChildInternalNodeKeys.size(), ""+leftChildInternalNodeKeys);
        Assertions.assertEquals(multi * 3, leftChildInternalNodeKeys.get(0));
        Assertions.assertEquals(multi * 5, leftChildInternalNodeKeys.get(1));

        // Far left leaf
        LeafClusterTreeNode<Long> currentLeaf = (LeafClusterTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        leftChildInternalNode.getChildPointersList(degree).get(0).getLeft(), KV_SIZE
                ).get().bytes()
        );
        List<Long> currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 1, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 2, currentLeafKeys.get(1));

        // 2nd Leaf
        Optional<Pointer> nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), leftChildInternalNode.getChildPointersList(degree).get(0).getRight());

        currentLeaf = (LeafClusterTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        leftChildInternalNode.getChildPointersList(degree).get(0).getRight(), KV_SIZE
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 3, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 4, currentLeafKeys.get(1));

        //3rd leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), leftChildInternalNode.getChildPointersList(degree).get(1).getRight());

        currentLeaf = (LeafClusterTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        leftChildInternalNode.getChildPointersList(degree).get(1).getRight(), KV_SIZE
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 5, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 6, currentLeafKeys.get(1));


        // Checking root child at right
        InternalTreeNode<Long> rightChildInternalNode = (InternalTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        ((InternalTreeNode<Long>) rootNode).getChildPointersList(degree).get(0).getRight(),
                        KV_SIZE
                ).get().bytes()
        );
        List<Long> rightChildInternalNodeKeys = rightChildInternalNode.getKeyList(degree);
        Assertions.assertEquals(2, rightChildInternalNodeKeys.size());
        Assertions.assertEquals(multi * 9, rightChildInternalNodeKeys.get(0));
        Assertions.assertEquals(multi * 11, rightChildInternalNodeKeys.get(1));

        // 4th leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNode.getChildPointersList(degree).get(0).getLeft());

        currentLeaf = (LeafClusterTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        rightChildInternalNode.getChildPointersList(degree).get(0).getLeft(),
                        KV_SIZE
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 7, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 8, currentLeafKeys.get(1));


        // 5th leaf
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNode.getChildPointersList(degree).get(0).getRight());

        currentLeaf = (LeafClusterTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        rightChildInternalNode.getChildPointersList(degree).get(0).getRight(),
                        KV_SIZE
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 9, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 10, currentLeafKeys.get(1));


        // 6th node
        nextPointer = currentLeaf.getNextSiblingPointer(degree);
        Assertions.assertTrue(nextPointer.isPresent());
        Assertions.assertEquals(nextPointer.get(), rightChildInternalNode.getChildPointersList(degree).get(1).getRight());

        currentLeaf = (LeafClusterTreeNode<Long>) nodeFactory.fromBytes(
                indexStorageManager.readNode(
                        table,
                        rightChildInternalNode.getChildPointersList(degree).get(1).getRight(),
                        KV_SIZE
                ).get().bytes()
        );
        currentLeafKeys = currentLeaf.getKeyList(degree);
        Assertions.assertEquals(2, currentLeafKeys.size());
        Assertions.assertEquals(multi * 11, currentLeafKeys.get(0));
        Assertions.assertEquals(multi * 12, currentLeafKeys.get(1));
    }

}
