package com.github.sepgh.test.index.tree.removing;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.KVSize;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexTreeNodeIO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;
import static com.github.sepgh.test.TestParams.DEFAULT_KV_SIZE;
import static com.github.sepgh.testudo.storage.index.BaseFileIndexStorageManager.INDEX_FILE_NAME;

public class BaseBPlusTreeUniqueTreeIndexManagerRemovalTestCase {
    protected Path dbPath;
    protected EngineConfig engineConfig;
    protected int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BaseBPlusTreeIndexManagerRemovalTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .baseDBPath(dbPath.toString())
                .build();
        engineConfig.setBTreeMaxFileSize(15L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size()));

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    /* 007
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
    public void testRemovingLeftToRight(UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager, IndexStorageManager indexStorageManager) throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (Long testIdentifier : testIdentifiers) {
            uniqueTreeIndexManager.addIndex(testIdentifier, samplePointer);
        }

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(1L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(1L));

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(2L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(2L));

        NodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        // Check Tree
        InternalTreeNode<Long> root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(7, root.getKeyList(degree).getFirst());

        InternalTreeNode<Long> midTreeNodeLeft = (InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        List<Long> midTreeNodeLeftKeyList = midTreeNodeLeft.getKeyList(degree);
        Assertions.assertEquals(2, midTreeNodeLeftKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(4, midTreeNodeLeftKeyList.getFirst(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(5, midTreeNodeLeftKeyList.getLast(), "" + midTreeNodeLeftKeyList);


        List<Pointer> midTreeNodeLeftChildrenList = midTreeNodeLeft.getChildrenList();
        LeafClusterTreeNode<Long> leftSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeLeftChildrenList.getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        List<Long> leftSideLastTreeNodeKeyList = leftSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, leftSideLastTreeNodeKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(3, leftSideLastTreeNodeKeyList.getFirst());


        LeafClusterTreeNode<Long> midLastTreeNodeAtLeft = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeLeftChildrenList.get(1), nodeFactory, DEFAULT_KV_SIZE);
        List<Long> midLastTreeNodeKeyList = midLastTreeNodeAtLeft.getKeyList(degree);
        Assertions.assertEquals(1, midLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNodeAtLeft.getPointer());
        Assertions.assertEquals(4, midLastTreeNodeKeyList.getFirst());


        LeafClusterTreeNode<Long> rightSideLastTreeNodeAtLeft = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeLeftChildrenList.getLast(), nodeFactory, DEFAULT_KV_SIZE);
        List<Long> rightSideLastTreeNodeKeyList = rightSideLastTreeNodeAtLeft.getKeyList(degree);
        Assertions.assertEquals(2, rightSideLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNodeAtLeft.getPointer());
        Assertions.assertEquals(5, rightSideLastTreeNodeKeyList.getFirst());
        Assertions.assertEquals(6, rightSideLastTreeNodeKeyList.getLast());


        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(3L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(3L));


        midTreeNodeLeft = (InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        midTreeNodeLeftKeyList = midTreeNodeLeft.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeLeftKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(5, midTreeNodeLeftKeyList.getFirst());


        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(4L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(4L));

        midTreeNodeLeft = (InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        midTreeNodeLeftKeyList = midTreeNodeLeft.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeLeftKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(6, midTreeNodeLeftKeyList.getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(5L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(5L));


        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(9, root.getKeyList(degree).getFirst());

        midTreeNodeLeft = (InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        midTreeNodeLeftKeyList = midTreeNodeLeft.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeLeftKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(7, midTreeNodeLeftKeyList.getFirst());

        midTreeNodeLeftChildrenList = midTreeNodeLeft.getChildrenList();

        leftSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeLeftChildrenList.getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        leftSideLastTreeNodeKeyList = leftSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, leftSideLastTreeNodeKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(6, leftSideLastTreeNodeKeyList.getFirst());

        rightSideLastTreeNodeAtLeft = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeLeftChildrenList.getLast(), nodeFactory, DEFAULT_KV_SIZE);
        rightSideLastTreeNodeKeyList = rightSideLastTreeNodeAtLeft.getKeyList(degree);
        Assertions.assertEquals(2, rightSideLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNodeAtLeft.getPointer());
        Assertions.assertEquals(7, rightSideLastTreeNodeKeyList.getFirst());
        Assertions.assertEquals(8, rightSideLastTreeNodeKeyList.getLast());



        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(6L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(6L));

        midTreeNodeLeft = (InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        midTreeNodeLeftKeyList = midTreeNodeLeft.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeLeftKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(8, midTreeNodeLeftKeyList.getFirst());


        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(7L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(7L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "" + root.getKeyList(degree));
        Assertions.assertEquals(9, root.getKeyList(degree).getFirst(), "" + root.getKeyList(degree));
        Assertions.assertEquals(11, root.getKeyList(degree).getLast(), "" + root.getKeyList(degree));

        AbstractTreeNode<Long> leafNodeAtLeft = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(1, leafNodeAtLeft.getKeyList(degree, PointerIndexBinaryObject.BYTES).size(), "" + leafNodeAtLeft.getKeyList(degree, PointerIndexBinaryObject.BYTES));
        Assertions.assertEquals(8, leafNodeAtLeft.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst(), "" + leafNodeAtLeft.getKeyList(degree, PointerIndexBinaryObject.BYTES));
        AbstractTreeNode<Long> leafNodeAtMid = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().get(1), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(2, leafNodeAtMid.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(9, leafNodeAtMid.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());
        Assertions.assertEquals(10, leafNodeAtMid.getKeyList(degree, PointerIndexBinaryObject.BYTES).getLast());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(8L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(8L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "" + root.getKeyList(degree));
        Assertions.assertEquals(10, root.getKeyList(degree).getFirst(), "" + root.getKeyList(degree));
        Assertions.assertEquals(11, root.getKeyList(degree).getLast(), "" + root.getKeyList(degree));

        leafNodeAtLeft = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(1, leafNodeAtLeft.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(9, leafNodeAtLeft.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());


        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(9L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(9L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(11, root.getKeyList(degree).getFirst());

        leafNodeAtLeft = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(1, leafNodeAtLeft.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(10, leafNodeAtLeft.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());

        LeafClusterTreeNode<Long> leafNodeAtRight = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(2, leafNodeAtRight.getKeyList(degree).size());
        Assertions.assertEquals(11, leafNodeAtRight.getKeyList(degree).getFirst());
        Assertions.assertEquals(12, leafNodeAtRight.getKeyList(degree).getLast());



        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(10L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(10L));



        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(12, root.getKeyList(degree).getFirst());

        leafNodeAtLeft = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(1, leafNodeAtLeft.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(11, leafNodeAtLeft.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());

        leafNodeAtRight = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(1, leafNodeAtRight.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(12, leafNodeAtRight.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());


        Assertions.assertTrue(uniqueTreeIndexManager.getIndex(11L).isPresent());
        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(11L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(11L));

        AbstractTreeNode<Long> bRoot = nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, bRoot.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(12, bRoot.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());
        Assertions.assertEquals(AbstractTreeNode.Type.LEAF, bRoot.getType());

        Assertions.assertTrue(uniqueTreeIndexManager.getIndex(12L).isPresent());
        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(12L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(12L));
        bRoot = nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(0, bRoot.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(AbstractTreeNode.Type.LEAF, bRoot.getType());

    }

    public void testRemovingRightToLeft(UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager, IndexStorageManager indexStorageManager) throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);
        NodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        for (Long testIdentifier : testIdentifiers) {
            uniqueTreeIndexManager.addIndex(testIdentifier, samplePointer);
        }

        Assertions.assertTrue(uniqueTreeIndexManager.getIndex(12L).isPresent());
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(13L));
        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(12L));


        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(12L));
        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(11L));

        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(11L));

        // Check Tree
        InternalTreeNode<Long> root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertTrue(root.getKeyList(degree).contains(7L));

        InternalTreeNode<Long> midTreeNode = (InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE);
        List<Long> midTreeNodeKeyList = midTreeNode.getKeyList(degree);
        Assertions.assertEquals(2, midTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(9, midTreeNodeKeyList.getFirst());
        Assertions.assertEquals(10, midTreeNodeKeyList.getLast());


        List<Pointer> midTreeNodeChildrenList = midTreeNode.getChildrenList();
        LeafClusterTreeNode<Long> leftSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeChildrenList.getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        List<Long> leftSideLastTreeNodeKeyList = leftSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(2, leftSideLastTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(7, leftSideLastTreeNodeKeyList.getFirst());
        Assertions.assertEquals(8, leftSideLastTreeNodeKeyList.getLast());


        LeafClusterTreeNode<Long> midLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeChildrenList.get(1), nodeFactory, DEFAULT_KV_SIZE);
        List<Long> midLastTreeNodeKeyList = midLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, midLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNode.getPointer());
        Assertions.assertEquals(9, midLastTreeNodeKeyList.getFirst());


        LeafClusterTreeNode<Long> rightSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeChildrenList.getLast(), nodeFactory, DEFAULT_KV_SIZE);
        List<Long> rightSideLastTreeNodeKeyList = rightSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, rightSideLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNode.getPointer());
        Assertions.assertEquals(10, rightSideLastTreeNodeKeyList.getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(10L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(10L));

        midTreeNode = (InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE);
        midTreeNodeKeyList = midTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(9, midTreeNodeKeyList.getFirst());


        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(9L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(9L));

        midTreeNode = (InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE);
        midTreeNodeKeyList = midTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(8, midTreeNodeKeyList.getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(8L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(8L));


        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(5, root.getKeyList(degree).getFirst());

        midTreeNode = (InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE);
        midTreeNodeKeyList = midTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(7, midTreeNodeKeyList.getFirst());

        midTreeNodeChildrenList = midTreeNode.getChildrenList();

        leftSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeChildrenList.getFirst(), nodeFactory, DEFAULT_KV_SIZE);
        leftSideLastTreeNodeKeyList = leftSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(2, leftSideLastTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(5, leftSideLastTreeNodeKeyList.getFirst());
        Assertions.assertEquals(6, leftSideLastTreeNodeKeyList.getLast());

        rightSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeChildrenList.getLast(), nodeFactory, DEFAULT_KV_SIZE);
        rightSideLastTreeNodeKeyList = rightSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, rightSideLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNode.getPointer());
        Assertions.assertEquals(7, rightSideLastTreeNodeKeyList.getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(7L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(7L));

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(6L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(6L));


        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "Keys: " + root.getKeyList(degree));
        Assertions.assertEquals(3, root.getKeyList(degree).getFirst());
        Assertions.assertEquals(5, root.getKeyList(degree).getLast());

        AbstractTreeNode<Long> leafNodeAtRight = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(1, leafNodeAtRight.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(5, leafNodeAtRight.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());


        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(5L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(5L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(2, root.getKeyList(degree).size());
        Assertions.assertEquals(3, root.getKeyList(degree).getFirst());
        Assertions.assertEquals(4, root.getKeyList(degree).getLast());

        leafNodeAtRight = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(1, leafNodeAtRight.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(4, leafNodeAtRight.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());


        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(4L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(4L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(3, root.getKeyList(degree).getFirst());

        leafNodeAtRight = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(1, leafNodeAtRight.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(3, leafNodeAtRight.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());


        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(3L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(3L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(2, root.getKeyList(degree).getFirst());

        leafNodeAtRight = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE);
        Assertions.assertEquals(1, leafNodeAtRight.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(2, leafNodeAtRight.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.getIndex(2L).isPresent());
        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(2L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(2L));

        AbstractTreeNode<Long> bRoot = nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, bRoot.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(1, bRoot.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());
        Assertions.assertEquals(AbstractTreeNode.Type.LEAF, bRoot.getType());

        Assertions.assertTrue(uniqueTreeIndexManager.getIndex(1L).isPresent());
        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(1L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(1L));
        bRoot = nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(0, bRoot.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(AbstractTreeNode.Type.LEAF, bRoot.getType());

    }


    public void testRemovingRoot(UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager, IndexStorageManager indexStorageManager) throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);
        NodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        for (Long testIdentifier : testIdentifiers) {
            uniqueTreeIndexManager.addIndex(testIdentifier, samplePointer);
        }

        // Check Tree
        InternalTreeNode<Long> root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(7, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(7L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(7L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(8, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(8L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(8L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(9, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(9L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(9L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(10, root.getKeyList(degree).getFirst());

        // Testing next / prev state
        Pointer lastLeafFromLeftPointer = ((InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), nodeFactory, DEFAULT_KV_SIZE)).getChildrenList().getLast();
        Pointer firstLeafFromRightPointer = ((InternalTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), nodeFactory, DEFAULT_KV_SIZE)).getChildrenList().getFirst();

        Assertions.assertEquals(lastLeafFromLeftPointer, ((LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, firstLeafFromRightPointer, nodeFactory, DEFAULT_KV_SIZE)).getPreviousSiblingPointer(degree).get());
        Assertions.assertEquals(firstLeafFromRightPointer, ((LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, lastLeafFromLeftPointer, nodeFactory, DEFAULT_KV_SIZE)).getNextSiblingPointer(degree).get());


        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(10L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(10L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(11, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(11L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(11L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(5, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(5L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(5L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(6, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(6L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(6L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(3, root.getKeyList(degree).getFirst());
        Assertions.assertEquals(12, root.getKeyList(degree).getLast());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(3L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(3L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(4, root.getKeyList(degree).getFirst());
        Assertions.assertEquals(12, root.getKeyList(degree).getLast());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(4L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(4L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(2, root.getKeyList(degree).getFirst());
        Assertions.assertEquals(12, root.getKeyList(degree).getLast());

        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(2L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(2L));

        root = (InternalTreeNode<Long>) nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(12, root.getKeyList(degree).getFirst());


        Assertions.assertTrue(uniqueTreeIndexManager.removeIndex(12L));
        Assertions.assertFalse(uniqueTreeIndexManager.removeIndex(12L));

        AbstractTreeNode<Long> lRoot = nodeFactory.fromNodeData(indexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().get());
        Assertions.assertEquals(1, lRoot.getKeyList(degree, PointerIndexBinaryObject.BYTES).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(1, lRoot.getKeyList(degree, PointerIndexBinaryObject.BYTES).getFirst());
        Assertions.assertEquals(AbstractTreeNode.Type.LEAF, lRoot.getType());

    }

    public void testRemovingLeftToRightAsync(UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager) throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        CountDownLatch countDownLatch = new CountDownLatch(testIdentifiers.size());
        for (Long testIdentifier : testIdentifiers) {
            executorService.submit(() -> {
                try {
                    uniqueTreeIndexManager.addIndex(testIdentifier, samplePointer);
                } catch (InternalOperationException e) {
                    throw new RuntimeException(e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        executorService.shutdown();

        executorService = Executors.newFixedThreadPool(4);
        CountDownLatch countDownLatch2 = new CountDownLatch(testIdentifiers.size());
        for (Long testIdentifier : testIdentifiers) {
            executorService.submit(() -> {
                try {
                    uniqueTreeIndexManager.removeIndex(testIdentifier);
                } catch (InternalOperationException e) {
                    throw new RuntimeException(e);
                } finally {
                    countDownLatch2.countDown();
                }
            });
        }

        countDownLatch2.await();
        executorService.shutdown();

        for (Long testIdentifier : testIdentifiers) {
            Assertions.assertTrue(uniqueTreeIndexManager.getIndex(testIdentifier).isEmpty(), "Still can get " + testIdentifier);
        }
    }
}
