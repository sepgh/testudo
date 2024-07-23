package com.github.sepgh.test.index.tree.storing;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.AsyncIndexManagerDecorator;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.PointerImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.CompactFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.github.sepgh.testudo.utils.KVSize;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.sepgh.testudo.storage.index.CompactFileIndexStorageManager.INDEX_FILE_NAME;

public class BPlusTreeIndexManagerTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BTreeIndexManagerTestCase");
        engineConfig = EngineConfig.builder()
                .baseDBPath(dbPath.toString())
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(4L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void destroy() throws IOException {
        Path indexPath0 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.delete(indexPath0);
        try {
            Path indexPath1 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
            Files.delete(indexPath1);
        } catch (NoSuchFileException ignored){}
    }

    private CompactFileIndexStorageManager getCompactFileIndexStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new CompactFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    @Test
    @Timeout(value = 2)
    public void addIndex() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        CompactFileIndexStorageManager compactFileIndexStorageManager = getCompactFileIndexStorageManager();

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(1, degree, compactFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());
        AbstractTreeNode<Long> baseClusterTreeNode = indexManager.addIndex( 10L, new Pointer(Pointer.TYPE_DATA, 100, 0));

        Assertions.assertTrue(baseClusterTreeNode.isRoot());
        Assertions.assertEquals(0, baseClusterTreeNode.getPointer().getPosition());
        Assertions.assertEquals(0, baseClusterTreeNode.getPointer().getChunk());

        IndexStorageManager.NodeData nodeData = compactFileIndexStorageManager.readNode(1, baseClusterTreeNode.getPointer(), new KVSize(LongImmutableBinaryObjectWrapper.BYTES, PointerImmutableBinaryObjectWrapper.BYTES)).get();
        LeafClusterTreeNode<Long> leafTreeNode = new LeafClusterTreeNode<>(nodeData.bytes(), new LongImmutableBinaryObjectWrapper());
        Assertions.assertTrue(leafTreeNode.isRoot());
        Iterator<LeafClusterTreeNode.KeyValue<Long, Pointer>> entryIterator = leafTreeNode.getKeyValues(degree);
        Assertions.assertTrue(entryIterator.hasNext());
        LeafClusterTreeNode.KeyValue<Long, Pointer> pointerEntry = entryIterator.next();
        Assertions.assertEquals(pointerEntry.key(), 10);
        Assertions.assertEquals(pointerEntry.value().getPosition(), 100);
    }

    @Test
    @Timeout(value = 2)
    public void testSingleSplitAddIndex() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        Random random = new Random();

        List<Long> testIdentifiers = new ArrayList<>(degree);
        int i = 0;
        while (testIdentifiers.size() < degree){
            long l = random.nextLong() % 100;
            if (!testIdentifiers.contains(l)){
                testIdentifiers.add(l);
                i++;
            }
        }

        Assertions.assertEquals(degree, i);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);


        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(new LongImmutableBinaryObjectWrapper());
        CompactFileIndexStorageManager compactFileIndexStorageManager = getCompactFileIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(1, degree, compactFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());


        AbstractTreeNode<Long> lastTreeNode = null;
        for (Long testIdentifier : testIdentifiers) {
            lastTreeNode = indexManager.addIndex(testIdentifier, samplePointer);
        }

        Assertions.assertTrue(lastTreeNode.isLeaf());
        Assertions.assertEquals(2, lastTreeNode.getKeyList(degree, PointerImmutableBinaryObjectWrapper.BYTES).size());
        Assertions.assertEquals(samplePointer.getPosition(), ((AbstractLeafTreeNode<Long, Pointer>) lastTreeNode).getKeyValues(degree).next().value().getPosition());

        Optional<IndexStorageManager.NodeData> optional = compactFileIndexStorageManager.getRoot(1, new KVSize(LongImmutableBinaryObjectWrapper.BYTES, PointerImmutableBinaryObjectWrapper.BYTES)).get();
        Assertions.assertTrue(optional.isPresent());

        AbstractTreeNode<Long> rootNode = nodeFactory.fromBytes(optional.get().bytes());
        Assertions.assertTrue(rootNode.isRoot());
        Assertions.assertFalse(rootNode.isLeaf());

        Assertions.assertEquals(1, rootNode.getKeyList(degree, PointerImmutableBinaryObjectWrapper.BYTES).size());

        testIdentifiers.sort(Long::compareTo);

        List<InternalTreeNode.ChildPointers<Long>> children = ((InternalTreeNode<Long>) rootNode).getChildPointersList(degree);
        Assertions.assertEquals(1, children.size());
        Assertions.assertNotNull(children.get(0).getLeft());
        Assertions.assertNotNull(children.get(0).getRight());
        Assertions.assertEquals(testIdentifiers.get(2), rootNode.getKeyList(degree, PointerImmutableBinaryObjectWrapper.BYTES).get(0));

        // First child
        LeafClusterTreeNode<Long> childLeafTreeNode = new LeafClusterTreeNode<>(compactFileIndexStorageManager.readNode(1, children.get(0).getLeft(), new KVSize(LongImmutableBinaryObjectWrapper.BYTES, PointerImmutableBinaryObjectWrapper.BYTES)).get().bytes(), new LongImmutableBinaryObjectWrapper());
        List<LeafClusterTreeNode.KeyValue<Long, Pointer>> keyValueList = childLeafTreeNode.getKeyValueList(degree);
        Assertions.assertEquals(testIdentifiers.get(0), keyValueList.get(0).key());
        Assertions.assertEquals(testIdentifiers.get(1), keyValueList.get(1).key());

        //Second child
        LeafClusterTreeNode<Long> secondChildLeafTreeNode = new LeafClusterTreeNode<>(compactFileIndexStorageManager.readNode(1, children.get(0).getRight(), new KVSize(LongImmutableBinaryObjectWrapper.BYTES, PointerImmutableBinaryObjectWrapper.BYTES)).get().bytes(), new LongImmutableBinaryObjectWrapper());
        keyValueList = secondChildLeafTreeNode.getKeyValueList(degree);
        Assertions.assertEquals(testIdentifiers.get(2), keyValueList.get(0).key());
        Assertions.assertEquals(testIdentifiers.get(3), keyValueList.get(1).key());

        Assertions.assertTrue(childLeafTreeNode.getNextSiblingPointer(degree).isPresent());
        Assertions.assertEquals(children.get(0).getRight(), childLeafTreeNode.getNextSiblingPointer(degree).get());

        Assertions.assertTrue(secondChildLeafTreeNode.getPreviousSiblingPointer(degree).isPresent());
        Assertions.assertEquals(children.get(0).getLeft(), secondChildLeafTreeNode.getPreviousSiblingPointer(degree).get());

    }

    /**
     *
     * The B+Tree in this test will include numbers from [1-12] added respectively
     * The shape of the tree will be like below, and the test verifies that
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
    @Test
    @Timeout(value = 2)
    public void testMultiSplitAddIndex() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);


        CompactFileIndexStorageManager compactFileIndexStorageManager = getCompactFileIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(1, degree, compactFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());


        AbstractTreeNode<Long> lastTreeNode = null;
        for (Long testIdentifier : testIdentifiers) {
            lastTreeNode = indexManager.addIndex(testIdentifier, samplePointer);
        }

        Assertions.assertTrue(lastTreeNode.isLeaf());
        Assertions.assertEquals(2, lastTreeNode.getKeyList(degree, PointerImmutableBinaryObjectWrapper.BYTES).size());
        Assertions.assertEquals(samplePointer.getPosition(), ((AbstractLeafTreeNode<Long, Pointer>) lastTreeNode).getKeyValues(degree).next().value().getPosition());

        StoredTreeStructureVerifier.testOrderedTreeStructure(compactFileIndexStorageManager, 1, 1, degree);

    }

    @Test
    @Timeout(value = 2)
    public void testMultiSplitAddIndexAsync() throws IOException, ExecutionException, InterruptedException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);


        CompactFileIndexStorageManager compactFileIndexStorageManager = getCompactFileIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = new AsyncIndexManagerDecorator<>(
                new ClusterBPlusTreeIndexManager<>(1, degree, compactFileIndexStorageManager, new LongImmutableBinaryObjectWrapper())
        );

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        CountDownLatch countDownLatch = new CountDownLatch(testIdentifiers.size());
        for (Long testIdentifier : testIdentifiers) {
            executorService.submit(() -> {
                try {
                    indexManager.addIndex(testIdentifier, samplePointer);
                    countDownLatch.countDown();
                } catch (ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue | IndexExistsException |
                         InternalOperationException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        countDownLatch.await();
        executorService.shutdown();

        for (Long testIdentifier : testIdentifiers) {
            Assertions.assertTrue(indexManager.getIndex(testIdentifier).isPresent());
        }

    }

}
