package com.github.sepgh.test.index.tree.storing;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.KVSize;
import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.InternalTreeNode;
import com.github.sepgh.testudo.index.tree.node.NodeFactory;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;
import static com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager.INDEX_FILE_NAME;

public class BPlusTreeUniqueTreeIndexManagerTestCase {
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
        engineConfig.setBTreeMaxFileSize(4L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size()));

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

    private OrganizedFileIndexStorageManager getCompactFileIndexStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new OrganizedFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    @Test
    @Timeout(value = 2)
    public void addIndex() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();

        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        AbstractTreeNode<Long> baseClusterTreeNode = uniqueTreeIndexManager.addIndex( 10L, new Pointer(Pointer.TYPE_DATA, 100, 0));

        Assertions.assertTrue(baseClusterTreeNode.isRoot());
        Assertions.assertEquals(0, baseClusterTreeNode.getPointer().getPosition());
        Assertions.assertEquals(0, baseClusterTreeNode.getPointer().getChunk());

        IndexStorageManager.NodeData nodeData = organizedFileIndexStorageManager.readNode(1, baseClusterTreeNode.getPointer(), new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get();
        LeafClusterTreeNode<Long> leafTreeNode = new LeafClusterTreeNode<>(nodeData.bytes(), DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        Assertions.assertTrue(leafTreeNode.isRoot());
        Iterator<KeyValue<Long, Pointer>> entryIterator = leafTreeNode.getKeyValues(degree);
        Assertions.assertTrue(entryIterator.hasNext());
        KeyValue<Long, Pointer> pointerEntry = entryIterator.next();
        Assertions.assertEquals(pointerEntry.key(), 10);
        Assertions.assertEquals(pointerEntry.value().getPosition(), 100);
    }

    @Test
    @Timeout(value = 2)
    public void testSingleSplitAddIndex() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
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


        NodeFactory.ClusterNodeFactory<Long> nodeFactory = new NodeFactory.ClusterNodeFactory<>(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());


        AbstractTreeNode<Long> lastTreeNode = null;
        for (Long testIdentifier : testIdentifiers) {
            lastTreeNode = uniqueTreeIndexManager.addIndex(testIdentifier, samplePointer);
        }

        Assertions.assertTrue(lastTreeNode.isLeaf());
        Assertions.assertEquals(2, lastTreeNode.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(samplePointer.getPosition(), ((AbstractLeafTreeNode<Long, Pointer>) lastTreeNode).getKeyValues(degree).next().value().getPosition());

        Optional<IndexStorageManager.NodeData> optional = organizedFileIndexStorageManager.getRoot(1, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get();
        Assertions.assertTrue(optional.isPresent());

        AbstractTreeNode<Long> rootNode = nodeFactory.fromBytes(optional.get().bytes());
        Assertions.assertTrue(rootNode.isRoot());
        Assertions.assertFalse(rootNode.isLeaf());

        Assertions.assertEquals(1, rootNode.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());

        testIdentifiers.sort(Long::compareTo);

        List<InternalTreeNode.ChildPointers<Long>> children = ((InternalTreeNode<Long>) rootNode).getChildPointersList(degree);
        Assertions.assertEquals(1, children.size());
        Assertions.assertNotNull(children.get(0).getLeft());
        Assertions.assertNotNull(children.get(0).getRight());
        Assertions.assertEquals(testIdentifiers.get(2), rootNode.getKeyList(degree, PointerIndexBinaryObject.BYTES).get(0));

        // First child
        LeafClusterTreeNode<Long> childLeafTreeNode = new LeafClusterTreeNode<>(organizedFileIndexStorageManager.readNode(1, children.get(0).getLeft(), new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().bytes(), DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        List<KeyValue<Long, Pointer>> keyValueList = childLeafTreeNode.getKeyValueList(degree);
        Assertions.assertEquals(testIdentifiers.get(0), keyValueList.get(0).key());
        Assertions.assertEquals(testIdentifiers.get(1), keyValueList.get(1).key());

        //Second child
        LeafClusterTreeNode<Long> secondChildLeafTreeNode = new LeafClusterTreeNode<>(organizedFileIndexStorageManager.readNode(1, children.get(0).getRight(), new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get().bytes(), DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
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
    public void testMultiSplitAddIndex() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);


        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());


        AbstractTreeNode<Long> lastTreeNode = null;
        for (Long testIdentifier : testIdentifiers) {
            lastTreeNode = uniqueTreeIndexManager.addIndex(testIdentifier, samplePointer);
        }

        Assertions.assertTrue(lastTreeNode.isLeaf());
        Assertions.assertEquals(2, lastTreeNode.getKeyList(degree, PointerIndexBinaryObject.BYTES).size());
        Assertions.assertEquals(samplePointer.getPosition(), ((AbstractLeafTreeNode<Long, Pointer>) lastTreeNode).getKeyValues(degree).next().value().getPosition());

        StoredTreeStructureVerifier.testOrderedTreeStructure(organizedFileIndexStorageManager, 1, 1, degree);

    }

}
