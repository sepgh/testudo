package com.github.sepgh.internal.index.tree.removing;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterIdentifier;
import com.github.sepgh.internal.index.tree.node.cluster.InternalClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.IndexTreeNodeIO;
import com.github.sepgh.internal.storage.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.sepgh.internal.storage.BaseFileIndexStorageManager.INDEX_FILE_NAME;

public class BaseBPlusTreeIndexManagerRemovalTestCase {
    protected Path dbPath;
    protected EngineConfig engineConfig;
    protected Header header;
    protected int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BaseBPlusTreeIndexManagerRemovalTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(15L * engineConfig.getPaddedSize());

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        header = Header.builder()
                .database("sample")
                .tables(
                        Collections.singletonList(
                                Header.Table.builder()
                                        .id(1)
                                        .name("test")
                                        .chunks(
                                                Collections.singletonList(
                                                        Header.IndexChunk.builder()
                                                                .chunk(0)
                                                                .offset(0)
                                                                .build()
                                                )
                                        )
                                        .initialized(true)
                                        .build()
                        )
                )
                .build();

        Assertions.assertTrue(header.getTableOfId(1).isPresent());
        Assertions.assertTrue(header.getTableOfId(1).get().getIndexChunk(0).isPresent());
    }

    @AfterEach
    public void destroy() throws IOException {
        Path indexPath0 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.delete(indexPath0);
        try {
            Path indexPath1 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 1));
            Files.delete(indexPath1);
        } catch (NoSuchFileException ignored){}
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
    public void testRemovingLeftToRight(IndexManager<Long> indexManager, IndexStorageManager indexStorageManager) throws IOException, ExecutionException, InterruptedException {
        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (Long testIdentifier : testIdentifiers) {
            indexManager.addIndex(1, testIdentifier, samplePointer);
        }

        Assertions.assertTrue(indexManager.removeIndex(1, 1L));
        Assertions.assertFalse(indexManager.removeIndex(1, 1L));

        Assertions.assertTrue(indexManager.removeIndex(1, 2L));
        Assertions.assertFalse(indexManager.removeIndex(1, 2L));


        // Check Tree
        InternalClusterTreeNode<Long> root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(7, root.getKeyList(degree).getFirst());

        InternalClusterTreeNode<Long> midTreeNodeLeft = (InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), ClusterIdentifier.LONG);
        List<Long> midTreeNodeLeftKeyList = midTreeNodeLeft.getKeyList(degree);
        Assertions.assertEquals(2, midTreeNodeLeftKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(4, midTreeNodeLeftKeyList.getFirst(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(5, midTreeNodeLeftKeyList.getLast(), "" + midTreeNodeLeftKeyList);


        List<Pointer> midTreeNodeLeftChildrenList = midTreeNodeLeft.getChildrenList();
        LeafClusterTreeNode<Long> leftSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeLeftChildrenList.getFirst(), ClusterIdentifier.LONG);
        List<Long> leftSideLastTreeNodeKeyList = leftSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, leftSideLastTreeNodeKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(3, leftSideLastTreeNodeKeyList.getFirst());


        LeafClusterTreeNode<Long> midLastTreeNodeAtLeft = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeLeftChildrenList.get(1), ClusterIdentifier.LONG);
        List<Long> midLastTreeNodeKeyList = midLastTreeNodeAtLeft.getKeyList(degree);
        Assertions.assertEquals(1, midLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNodeAtLeft.getPointer());
        Assertions.assertEquals(4, midLastTreeNodeKeyList.getFirst());


        LeafClusterTreeNode<Long> rightSideLastTreeNodeAtLeft = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeLeftChildrenList.getLast(), ClusterIdentifier.LONG);
        List<Long> rightSideLastTreeNodeKeyList = rightSideLastTreeNodeAtLeft.getKeyList(degree);
        Assertions.assertEquals(2, rightSideLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNodeAtLeft.getPointer());
        Assertions.assertEquals(5, rightSideLastTreeNodeKeyList.getFirst());
        Assertions.assertEquals(6, rightSideLastTreeNodeKeyList.getLast());


        Assertions.assertTrue(indexManager.removeIndex(1, 3L));
        Assertions.assertFalse(indexManager.removeIndex(1, 3L));


        midTreeNodeLeft = (InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), ClusterIdentifier.LONG);
        midTreeNodeLeftKeyList = midTreeNodeLeft.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeLeftKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(5, midTreeNodeLeftKeyList.getFirst());


        Assertions.assertTrue(indexManager.removeIndex(1, 4L));
        Assertions.assertFalse(indexManager.removeIndex(1, 4L));

        midTreeNodeLeft = (InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), ClusterIdentifier.LONG);
        midTreeNodeLeftKeyList = midTreeNodeLeft.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeLeftKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(6, midTreeNodeLeftKeyList.getFirst());

        Assertions.assertTrue(indexManager.removeIndex(1, 5L));
        Assertions.assertFalse(indexManager.removeIndex(1, 5L));


        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(9, root.getKeyList(degree).getFirst());

        midTreeNodeLeft = (InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), ClusterIdentifier.LONG);
        midTreeNodeLeftKeyList = midTreeNodeLeft.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeLeftKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(7, midTreeNodeLeftKeyList.getFirst());

        midTreeNodeLeftChildrenList = midTreeNodeLeft.getChildrenList();

        leftSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeLeftChildrenList.getFirst(), ClusterIdentifier.LONG);
        leftSideLastTreeNodeKeyList = leftSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, leftSideLastTreeNodeKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(6, leftSideLastTreeNodeKeyList.getFirst());

        rightSideLastTreeNodeAtLeft = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeLeftChildrenList.getLast(), ClusterIdentifier.LONG);
        rightSideLastTreeNodeKeyList = rightSideLastTreeNodeAtLeft.getKeyList(degree);
        Assertions.assertEquals(2, rightSideLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNodeAtLeft.getPointer());
        Assertions.assertEquals(7, rightSideLastTreeNodeKeyList.getFirst());
        Assertions.assertEquals(8, rightSideLastTreeNodeKeyList.getLast());



        Assertions.assertTrue(indexManager.removeIndex(1, 6L));
        Assertions.assertFalse(indexManager.removeIndex(1, 6L));

        midTreeNodeLeft = (InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), ClusterIdentifier.LONG);
        midTreeNodeLeftKeyList = midTreeNodeLeft.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeLeftKeyList.size(), "" + midTreeNodeLeftKeyList);
        Assertions.assertEquals(8, midTreeNodeLeftKeyList.getFirst());


        Assertions.assertTrue(indexManager.removeIndex(1, 7L));
        Assertions.assertFalse(indexManager.removeIndex(1, 7L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "" + root.getKeyList(degree));
        Assertions.assertEquals(9, root.getKeyList(degree).getFirst(), "" + root.getKeyList(degree));
        Assertions.assertEquals(11, root.getKeyList(degree).getLast(), "" + root.getKeyList(degree));

        BaseClusterTreeNode<Long> leafNodeAtLeft = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, leafNodeAtLeft.getKeyList(degree).size(), "" + leafNodeAtLeft.getKeyList(degree));
        Assertions.assertEquals(8, leafNodeAtLeft.getKeyList(degree).getFirst(), "" + leafNodeAtLeft.getKeyList(degree));
        BaseClusterTreeNode<Long> leafNodeAtMid = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().get(1), ClusterIdentifier.LONG);
        Assertions.assertEquals(2, leafNodeAtMid.getKeyList(degree).size());
        Assertions.assertEquals(9, leafNodeAtMid.getKeyList(degree).getFirst());
        Assertions.assertEquals(10, leafNodeAtMid.getKeyList(degree).getLast());

        Assertions.assertTrue(indexManager.removeIndex(1, 8L));
        Assertions.assertFalse(indexManager.removeIndex(1, 8L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "" + root.getKeyList(degree));
        Assertions.assertEquals(10, root.getKeyList(degree).getFirst(), "" + root.getKeyList(degree));
        Assertions.assertEquals(11, root.getKeyList(degree).getLast(), "" + root.getKeyList(degree));

        leafNodeAtLeft = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, leafNodeAtLeft.getKeyList(degree).size());
        Assertions.assertEquals(9, leafNodeAtLeft.getKeyList(degree).getFirst());


        Assertions.assertTrue(indexManager.removeIndex(1, 9L));
        Assertions.assertFalse(indexManager.removeIndex(1, 9L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(11, root.getKeyList(degree).getFirst());

        leafNodeAtLeft = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, leafNodeAtLeft.getKeyList(degree).size());
        Assertions.assertEquals(10, leafNodeAtLeft.getKeyList(degree).getFirst());

        LeafClusterTreeNode<Long> leafNodeAtRight = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG);
        Assertions.assertEquals(2, leafNodeAtRight.getKeyList(degree).size());
        Assertions.assertEquals(11, leafNodeAtRight.getKeyList(degree).getFirst());
        Assertions.assertEquals(12, leafNodeAtRight.getKeyList(degree).getLast());



        Assertions.assertTrue(indexManager.removeIndex(1, 10L));
        Assertions.assertFalse(indexManager.removeIndex(1, 10L));



        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(12, root.getKeyList(degree).getFirst());

        leafNodeAtLeft = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, leafNodeAtLeft.getKeyList(degree).size());
        Assertions.assertEquals(11, leafNodeAtLeft.getKeyList(degree).getFirst());

        leafNodeAtRight = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, leafNodeAtRight.getKeyList(degree).size());
        Assertions.assertEquals(12, leafNodeAtRight.getKeyList(degree).getFirst());


        Assertions.assertTrue(indexManager.getIndex(1, 11L).isPresent());
        Assertions.assertTrue(indexManager.removeIndex(1, 11L));
        Assertions.assertFalse(indexManager.removeIndex(1, 11L));

        BaseClusterTreeNode<Long> bRoot = InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, bRoot.getKeyList(degree).size());
        Assertions.assertEquals(12, bRoot.getKeyList(degree).getFirst());
        Assertions.assertEquals(BaseClusterTreeNode.Type.LEAF, bRoot.getType());

        Assertions.assertTrue(indexManager.getIndex(1, 12L).isPresent());
        Assertions.assertTrue(indexManager.removeIndex(1, 12L));
        Assertions.assertFalse(indexManager.removeIndex(1, 12L));
        bRoot = InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(0, bRoot.getKeyList(degree).size());
        Assertions.assertEquals(BaseClusterTreeNode.Type.LEAF, bRoot.getType());

    }

    public void testRemovingRightToLeft(IndexManager<Long> indexManager, IndexStorageManager indexStorageManager) throws IOException, ExecutionException, InterruptedException {
        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);


        for (Long testIdentifier : testIdentifiers) {
            indexManager.addIndex(1, testIdentifier, samplePointer);
        }

        Assertions.assertFalse(indexManager.removeIndex(1, 13L));
        Assertions.assertTrue(indexManager.removeIndex(1, 12L));



        Assertions.assertFalse(indexManager.removeIndex(1, 12L));
        Assertions.assertTrue(indexManager.removeIndex(1, 11L));

        Assertions.assertFalse(indexManager.removeIndex(1, 11L));

        // Check Tree
        InternalClusterTreeNode<Long> root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertTrue(root.getKeyList(degree).contains(7L));

        InternalClusterTreeNode<Long> midTreeNode = (InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG);
        List<Long> midTreeNodeKeyList = midTreeNode.getKeyList(degree);
        Assertions.assertEquals(2, midTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(9, midTreeNodeKeyList.getFirst());
        Assertions.assertEquals(10, midTreeNodeKeyList.getLast());


        List<Pointer> midTreeNodeChildrenList = midTreeNode.getChildrenList();
        LeafClusterTreeNode<Long> leftSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeChildrenList.getFirst(), ClusterIdentifier.LONG);
        List<Long> leftSideLastTreeNodeKeyList = leftSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(2, leftSideLastTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(7, leftSideLastTreeNodeKeyList.getFirst());
        Assertions.assertEquals(8, leftSideLastTreeNodeKeyList.getLast());


        LeafClusterTreeNode<Long> midLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeChildrenList.get(1), ClusterIdentifier.LONG);
        List<Long> midLastTreeNodeKeyList = midLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, midLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNode.getPointer());
        Assertions.assertEquals(9, midLastTreeNodeKeyList.getFirst());


        LeafClusterTreeNode<Long> rightSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeChildrenList.getLast(), ClusterIdentifier.LONG);
        List<Long> rightSideLastTreeNodeKeyList = rightSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, rightSideLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNode.getPointer());
        Assertions.assertEquals(10, rightSideLastTreeNodeKeyList.getFirst());


        Assertions.assertTrue(indexManager.removeIndex(1, 10L));
        Assertions.assertFalse(indexManager.removeIndex(1, 10L));


        midTreeNode = (InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG);
        midTreeNodeKeyList = midTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(9, midTreeNodeKeyList.getFirst());


        Assertions.assertTrue(indexManager.removeIndex(1, 9L));
        Assertions.assertFalse(indexManager.removeIndex(1, 9L));

        midTreeNode = (InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG);
        midTreeNodeKeyList = midTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(8, midTreeNodeKeyList.getFirst());

        Assertions.assertTrue(indexManager.removeIndex(1, 8L));
        Assertions.assertFalse(indexManager.removeIndex(1, 8L));


        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(5, root.getKeyList(degree).getFirst());

        midTreeNode = (InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG);
        midTreeNodeKeyList = midTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, midTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(7, midTreeNodeKeyList.getFirst());

        midTreeNodeChildrenList = midTreeNode.getChildrenList();

        leftSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeChildrenList.getFirst(), ClusterIdentifier.LONG);
        leftSideLastTreeNodeKeyList = leftSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(2, leftSideLastTreeNodeKeyList.size(), "" + midTreeNodeKeyList);
        Assertions.assertEquals(5, leftSideLastTreeNodeKeyList.getFirst());
        Assertions.assertEquals(6, leftSideLastTreeNodeKeyList.getLast());

        rightSideLastTreeNode = (LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, midTreeNodeChildrenList.getLast(), ClusterIdentifier.LONG);
        rightSideLastTreeNodeKeyList = rightSideLastTreeNode.getKeyList(degree);
        Assertions.assertEquals(1, rightSideLastTreeNodeKeyList.size(), "Keys: " + midLastTreeNodeKeyList + ", P: " + midLastTreeNode.getPointer());
        Assertions.assertEquals(7, rightSideLastTreeNodeKeyList.getFirst());

        Assertions.assertTrue(indexManager.removeIndex(1, 7L));
        Assertions.assertFalse(indexManager.removeIndex(1, 7L));

        Assertions.assertTrue(indexManager.removeIndex(1, 6L));
        Assertions.assertFalse(indexManager.removeIndex(1, 6L));


        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(2, root.getKeyList(degree).size());
        Assertions.assertEquals(3, root.getKeyList(degree).getFirst());
        Assertions.assertEquals(5, root.getKeyList(degree).getLast());

        BaseClusterTreeNode<Long> leafNodeAtRight = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, leafNodeAtRight.getKeyList(degree).size());
        Assertions.assertEquals(5, leafNodeAtRight.getKeyList(degree).getFirst());


        Assertions.assertTrue(indexManager.removeIndex(1, 5L));
        Assertions.assertFalse(indexManager.removeIndex(1, 5L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(2, root.getKeyList(degree).size());
        Assertions.assertEquals(3, root.getKeyList(degree).getFirst());
        Assertions.assertEquals(4, root.getKeyList(degree).getLast());

        leafNodeAtRight = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, leafNodeAtRight.getKeyList(degree).size());
        Assertions.assertEquals(4, leafNodeAtRight.getKeyList(degree).getFirst());


        Assertions.assertTrue(indexManager.removeIndex(1, 4L));
        Assertions.assertFalse(indexManager.removeIndex(1, 4L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(3, root.getKeyList(degree).getFirst());

        leafNodeAtRight = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, leafNodeAtRight.getKeyList(degree).size());
        Assertions.assertEquals(3, leafNodeAtRight.getKeyList(degree).getFirst());


        Assertions.assertTrue(indexManager.removeIndex(1, 3L));
        Assertions.assertFalse(indexManager.removeIndex(1, 3L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(2, root.getKeyList(degree).getFirst());

        leafNodeAtRight = IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, leafNodeAtRight.getKeyList(degree).size());
        Assertions.assertEquals(2, leafNodeAtRight.getKeyList(degree).getFirst());

        Assertions.assertTrue(indexManager.getIndex(1, 2L).isPresent());
        Assertions.assertTrue(indexManager.removeIndex(1, 2L));
        Assertions.assertFalse(indexManager.removeIndex(1, 2L));

        BaseClusterTreeNode<Long> bRoot = InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, bRoot.getKeyList(degree).size());
        Assertions.assertEquals(1, bRoot.getKeyList(degree).getFirst());
        Assertions.assertEquals(BaseClusterTreeNode.Type.LEAF, bRoot.getType());

        Assertions.assertTrue(indexManager.getIndex(1, 1L).isPresent());
        Assertions.assertTrue(indexManager.removeIndex(1, 1L));
        Assertions.assertFalse(indexManager.removeIndex(1, 1L));
        bRoot = InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(0, bRoot.getKeyList(degree).size());
        Assertions.assertEquals(BaseClusterTreeNode.Type.LEAF, bRoot.getType());

    }


    public void testRemovingRoot(IndexManager<Long> indexManager, IndexStorageManager indexStorageManager) throws IOException, ExecutionException, InterruptedException {
        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (Long testIdentifier : testIdentifiers) {
            indexManager.addIndex(1, testIdentifier, samplePointer);
        }

        // Check Tree
        InternalClusterTreeNode<Long> root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(7, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(indexManager.removeIndex(1, 7L));
        Assertions.assertFalse(indexManager.removeIndex(1, 7L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(8, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(indexManager.removeIndex(1, 8L));
        Assertions.assertFalse(indexManager.removeIndex(1, 8L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(9, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(indexManager.removeIndex(1, 9L));
        Assertions.assertFalse(indexManager.removeIndex(1, 9L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(10, root.getKeyList(degree).getFirst());

        // Testing next / prev state
        Pointer lastLeafFromLeftPointer = ((InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getFirst(), ClusterIdentifier.LONG)).getChildrenList().getLast();
        Pointer firstLeafFromRightPointer = ((InternalClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, root.getChildrenList().getLast(), ClusterIdentifier.LONG)).getChildrenList().getFirst();

        Assertions.assertEquals(lastLeafFromLeftPointer, ((LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, firstLeafFromRightPointer, ClusterIdentifier.LONG)).getPreviousSiblingPointer(degree).get());
        Assertions.assertEquals(firstLeafFromRightPointer, ((LeafClusterTreeNode<Long>) IndexTreeNodeIO.read(indexStorageManager, 1, lastLeafFromLeftPointer, ClusterIdentifier.LONG)).getNextSiblingPointer(degree).get());


        Assertions.assertTrue(indexManager.removeIndex(1, 10L));
        Assertions.assertFalse(indexManager.removeIndex(1, 10L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size());
        Assertions.assertEquals(11, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(indexManager.removeIndex(1, 11L));
        Assertions.assertFalse(indexManager.removeIndex(1, 11L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(5, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(indexManager.removeIndex(1, 5L));
        Assertions.assertFalse(indexManager.removeIndex(1, 5L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(6, root.getKeyList(degree).getFirst());

        Assertions.assertTrue(indexManager.removeIndex(1, 6L));
        Assertions.assertFalse(indexManager.removeIndex(1, 6L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(3, root.getKeyList(degree).getFirst());
        Assertions.assertEquals(12, root.getKeyList(degree).getLast());

        Assertions.assertTrue(indexManager.removeIndex(1, 3L));
        Assertions.assertFalse(indexManager.removeIndex(1, 3L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(4, root.getKeyList(degree).getFirst());
        Assertions.assertEquals(12, root.getKeyList(degree).getLast());

        Assertions.assertTrue(indexManager.removeIndex(1, 4L));
        Assertions.assertFalse(indexManager.removeIndex(1, 4L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(2, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(2, root.getKeyList(degree).getFirst());
        Assertions.assertEquals(12, root.getKeyList(degree).getLast());

        Assertions.assertTrue(indexManager.removeIndex(1, 2L));
        Assertions.assertFalse(indexManager.removeIndex(1, 2L));

        root = (InternalClusterTreeNode<Long>) InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, root.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(12, root.getKeyList(degree).getFirst());


        Assertions.assertTrue(indexManager.removeIndex(1, 12L));
        Assertions.assertFalse(indexManager.removeIndex(1, 12L));

        BaseClusterTreeNode<Long> lRoot = InternalClusterTreeNode.fromNodeData(indexStorageManager.getRoot(1).get().get(), ClusterIdentifier.LONG);
        Assertions.assertEquals(1, lRoot.getKeyList(degree).size(), "" +  root.getKeyList(degree));
        Assertions.assertEquals(1, lRoot.getKeyList(degree).getFirst());
        Assertions.assertEquals(BaseClusterTreeNode.Type.LEAF, lRoot.getType());

    }

    public void testRemovingLeftToRightAsync(IndexManager<Long> indexManager) throws IOException, ExecutionException, InterruptedException {
        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        CountDownLatch countDownLatch = new CountDownLatch(testIdentifiers.size());
        for (Long testIdentifier : testIdentifiers) {
            executorService.submit(() -> {
                try {
                    indexManager.addIndex(1, testIdentifier, samplePointer);
                } catch (ExecutionException | InterruptedException | IOException e) {
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
                    indexManager.removeIndex(1, testIdentifier);
                } catch (ExecutionException | InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }finally {
                    countDownLatch2.countDown();
                }
            });
        }

        countDownLatch2.await();
        executorService.shutdown();

        for (Long testIdentifier : testIdentifiers) {
            Assertions.assertTrue(indexManager.getIndex(1, testIdentifier).isEmpty(), "Still can get " + testIdentifier);
        }
    }
}
