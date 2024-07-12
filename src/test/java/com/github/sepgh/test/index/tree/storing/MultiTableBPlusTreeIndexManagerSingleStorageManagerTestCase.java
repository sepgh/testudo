package com.github.sepgh.test.index.tree.storing;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.DBLevelAsyncIndexManagerDecorator;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.index.tree.node.AbstractTreeNode;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.PointerImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.SingleFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.*;

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
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.sepgh.testudo.storage.index.SingleFileIndexStorageManager.INDEX_FILE_NAME;

public class MultiTableBPlusTreeIndexManagerSingleStorageManagerTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private final int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_MultiTableBPlusTreeIndexManagerSingleStorageManagerTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(10)
                .build();
        engineConfig.setBTreeMaxFileSize(2 * 15L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        byte[] writingBytes = new byte[2 * 13 * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES)];
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.bin", INDEX_FILE_NAME));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    private SingleFileIndexStorageManager getSingleFileIndexStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new SingleFileIndexStorageManager(
                dbPath,
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }


    /**
     *
     * The B+Tree in this test will include numbers from [1-12] added respectively
     * The shape of the tree will be like below, and the test verifies that
     * The test validates the tree for 2 tables in same database
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
    public void testMultiSplitAddIndex() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (int tableId = 1; tableId <= 2; tableId++){
            SingleFileIndexStorageManager singleFileIndexStorageManager = getSingleFileIndexStorageManager();
            IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, singleFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());



            AbstractTreeNode<Long> lastTreeNode = null;
            for (long testIdentifier : testIdentifiers) {
                lastTreeNode = indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }

            Assertions.assertTrue(lastTreeNode.isLeaf());
            Assertions.assertEquals(2, lastTreeNode.getKeyList(degree, PointerImmutableBinaryObjectWrapper.BYTES).size());
            Assertions.assertEquals(samplePointer.getPosition(), ((AbstractLeafTreeNode<Long, Pointer>) lastTreeNode).getKeyValues(degree).next().value().getPosition());

            StoredTreeStructureVerifier.testOrderedTreeStructure(singleFileIndexStorageManager, tableId, 1, degree);
        }

    }


    /**
     *
     * The B+Tree in this test will include numbers from [1-12] added respectively
     * The shape of the tree will be like below, and the test verifies that
     * The test validates the tree for 2 tables in same database
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
    @Test
    public void testMultiSplitAddIndex2() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 4L, 9L, 6L, 10L, 8L, 3L, 2L, 11L, 5L, 7L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);
        SingleFileIndexStorageManager singleFileIndexStorageManager = getSingleFileIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, singleFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());

        for (int tableId = 1; tableId <= 2; tableId++){

            AbstractTreeNode<Long> lastTreeNode = null;
            for (long testIdentifier : testIdentifiers) {
                lastTreeNode = indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }

            Assertions.assertTrue(lastTreeNode.isLeaf());
            Assertions.assertEquals(2, lastTreeNode.getKeyList(degree, PointerImmutableBinaryObjectWrapper.BYTES).size());
            Assertions.assertEquals(samplePointer.getPosition(), ((AbstractLeafTreeNode<Long, Pointer>) lastTreeNode).getKeyValues(degree).next().value().getPosition());

            StoredTreeStructureVerifier.testUnOrderedTreeStructure1(singleFileIndexStorageManager, tableId, 1, degree);

        }

    }

    @Timeout(2)
    @Test
    public void testAddIndexDifferentAddOrdersOnDBLevelAsyncIndexManager_usingSingleFileIndexStorageManager() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        SingleFileIndexStorageManager singleFileIndexStorageManager = getSingleFileIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = new DBLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, singleFileIndexStorageManager, new LongImmutableBinaryObjectWrapper()));

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        CountDownLatch countDownLatch = new CountDownLatch((2 * testIdentifiers.size()) - 2);


        AtomicInteger index1 = new AtomicInteger(0);
        AtomicInteger index2 = new AtomicInteger(0);

        int runs = 0;
        while (runs < testIdentifiers.size()){
            executorService.submit(() -> {
                try {
                    indexManager.addIndex(1, testIdentifiers.get(index1.getAndIncrement()), samplePointer);
                    countDownLatch.countDown();
                } catch (ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue | IndexExistsException |
                         InternalOperationException e) {
                    throw new RuntimeException(e);
                }
            });
            executorService.submit(() -> {
                try {
                    indexManager.addIndex(2, testIdentifiers.get(index2.getAndIncrement()) * 10, samplePointer);
                    countDownLatch.countDown();
                } catch (ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue | IndexExistsException |
                         InternalOperationException e) {
                    throw new RuntimeException(e);
                }
            });
            runs++;
        }

        countDownLatch.await();

        for (int tableId = 1; tableId <= 2; tableId++) {

            int multi = 1;
            if (tableId == 2){
                multi = 10;
            }
            // Cant verify structure but can check if all index are added
            for (Long testIdentifier : testIdentifiers) {
                Assertions.assertTrue(indexManager.getIndex(tableId, testIdentifier * multi).isPresent());
            }
        }

    }

}
