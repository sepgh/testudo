package com.github.sepgh.testudo.index.tree.storing;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.DBLevelAsyncIndexManagerDecorator;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.ExtendedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.InMemoryHeaderManager;
import com.github.sepgh.testudo.storage.IndexStorageManager;
import com.github.sepgh.testudo.storage.header.Header;
import com.github.sepgh.testudo.storage.header.HeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.LimitedFileHandlerPool;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.sepgh.testudo.storage.ExtendedFileIndexStorageManager.INDEX_FILE_NAME;

/*
*  The purpose of this test case is to assure allocation wouldn't cause issue in multi-table environment using ExtendedFileIndexStorage
*  Additionally, new chunks should be created and not cause problem
*/
public class MultiTableBPlusTreeIndexManagerExtendedAllocationAndChunkTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private final int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_MultiTableBPlusTreeIndexManagerExtendedAllocationAndChunkTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .fileAcquireTimeout(1)
                .fileAcquireUnit(TimeUnit.SECONDS)
                .bTreeGrowthNodeAllocationCount(1)
                .build();
        engineConfig.setBTreeMaxFileSize(4L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        byte[] writingBytes = new byte[2 * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES)];
        Path indexPath = Path.of(dbPath.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, 1, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        indexPath = Path.of(dbPath.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, 2, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        header = Header.builder()
                .database("sample")
                .tables(
                        Arrays.asList(
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
                                        .root(
                                                Header.IndexChunk.builder()
                                                        .chunk(0)
                                                        .offset(0)
                                                        .build()
                                        )
                                        .initialized(true)
                                        .build(),
                                Header.Table.builder()
                                        .id(2)
                                        .name("test2")
                                        .chunks(
                                                Collections.singletonList(
                                                        Header.IndexChunk.builder()
                                                                .chunk(0)
                                                                .offset(2L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES))
                                                                .build()
                                                )
                                        )
                                        .root(
                                                Header.IndexChunk.builder()
                                                        .chunk(0)
                                                        .offset(0)
                                                        .build()
                                        )
                                        .initialized(true)
                                        .build()
                        )
                )
                .build();

        Assertions.assertTrue(header.getTableOfId(1).isPresent());
        Assertions.assertTrue(header.getTableOfId(1).get().getIndexChunk(0).isPresent());
        Assertions.assertTrue(header.getTableOfId(2).isPresent());
        Assertions.assertTrue(header.getTableOfId(2).get().getIndexChunk(0).isPresent());
    }

    @AfterEach
    public void destroy() {
        new File(dbPath.toString()).delete();
    }


    /**
     *
     * The B+Tree in this test will include numbers from [1-12] added respectively
     * The shape of the tree will be like below, and the test verifies that
     * The test validates the tree for 2 tables in same database
     * 007
     * ├── .
     * │   ├── 001   [LEAF NODE 1]
     * │   └── 002   [LEAF NODE 1]
     * ├── 003
     * │   ├── 003   [LEAF NODE 2]
     * │   └── 004   [LEAF NODE 2]
     * ├── 005
     * │   ├── 005   [LEAF NODE 3]
     * │   └── 006   [LEAF NODE 3]
     * ├── .
     * │   ├── 007   [LEAF NODE 4]
     * │   └── 008   [LEAF NODE 4]
     * ├── 009
     * │   ├── 009   [LEAF NODE 5]
     * │   └── 010   [LEAF NODE 5]
     * └── 0011
     *     ├── 011   [LEAF NODE 6]
     *     └── 012   [LEAF NODE 6]
     */
    @Test
    public void testMultiSplitAddIndex() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);
        ExtendedFileIndexStorageManager extendedFileIndexStorageManager = new ExtendedFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, extendedFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());

        for (int tableId = 1; tableId <= 2; tableId++){

            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }

            Optional<IndexStorageManager.NodeData> optional = extendedFileIndexStorageManager.getRoot(tableId).get();
            Assertions.assertTrue(optional.isPresent());
            Assertions.assertNotEquals(0, optional.get().pointer().getChunk());

            StoredTreeStructureVerifier.testOrderedTreeStructure(extendedFileIndexStorageManager, tableId, 1, degree);

        }

    }

    @Test
    public void testMultiSplitAddIndexLimitedOpenFiles_SuccessLimitTo1() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);


        LimitedFileHandlerPool limitedFileHandlerPool = new LimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance(), 1);
        ExtendedFileIndexStorageManager extendedFileIndexStorageManager = new ExtendedFileIndexStorageManager(dbPath, headerManager, engineConfig, limitedFileHandlerPool, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, extendedFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());

        for (int tableId = 1; tableId <= 2; tableId++) {
            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }
        }

        Path indexPath = Path.of(dbPath.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, 1, 0));
        FileHandler fileHandler = limitedFileHandlerPool.getFileHandler(indexPath.toString());
        Assertions.assertNull(fileHandler);

        indexPath = Path.of(dbPath.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, 1, 1));
        fileHandler = limitedFileHandlerPool.getFileHandler(indexPath.toString());
        Assertions.assertNull(fileHandler);
    }

    @Test
    public void testMultiSplitAddIndexLimitedOpenFiles_SuccessLimitTo2() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);


        LimitedFileHandlerPool limitedFileHandlerPool = new LimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance(),2);
        ExtendedFileIndexStorageManager extendedFileIndexStorageManager = new ExtendedFileIndexStorageManager(dbPath, headerManager, engineConfig, limitedFileHandlerPool, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, extendedFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());

        for (int tableId = 1; tableId <= 2; tableId++) {
            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }
        }


        Path indexPath = Path.of(dbPath.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, 1, 0));
        FileHandler fileHandler = limitedFileHandlerPool.getFileHandler(indexPath.toString());
        Assertions.assertNull(fileHandler);

        indexPath = Path.of(dbPath.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, 1, 1));
        fileHandler = limitedFileHandlerPool.getFileHandler(indexPath.toString());
        Assertions.assertNull(fileHandler);

    }


    @Timeout(10)
    @Test
    public void testMultiSplitAddIndexDifferentAddOrdersOnDBLevelAsyncIndexManager() throws IOException, ExecutionException, InterruptedException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);
        ExtendedFileIndexStorageManager extendedFileIndexStorageManager = new ExtendedFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));
        IndexManager<Long, Pointer> indexManager = new DBLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, extendedFileIndexStorageManager, new LongImmutableBinaryObjectWrapper()));

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

        HeaderManager headerManager = new InMemoryHeaderManager(header);
        ExtendedFileIndexStorageManager extendedFileIndexStorageManager = new ExtendedFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, extendedFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());

        for (int tableId = 1; tableId <= 2; tableId++){

            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }

            StoredTreeStructureVerifier.testUnOrderedTreeStructure1(extendedFileIndexStorageManager, tableId, 1, degree);

        }

    }

}
