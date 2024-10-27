package com.github.sepgh.test.index.tree.storing;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.LimitedFileHandlerPool;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.github.sepgh.testudo.utils.KVSize;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;
import static com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager.INDEX_FILE_NAME;

/*
*  The purpose of this test case is to assure allocation wouldn't cause issue in multi-table environment
*  Additionally, new chunks should be created and not cause problem
*/
public class MultiTableBPlusTreeUniqueTreeIndexManagerOrganizedAllocationAndChunkTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private final int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_MultiTableBTreeIndexManagerAllocationAndChunkTestCase");
        engineConfig = EngineConfig.builder()
                .baseDBPath(dbPath.toString())
                .bTreeDegree(degree)
                .fileAcquireTimeout(1)
                .fileAcquireUnit(TimeUnit.SECONDS)
                .bTreeGrowthNodeAllocationCount(1)
                .build();
        engineConfig.setBTreeMaxFileSize(2L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size()));

        byte[] writingBytes = new byte[6 * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size())];
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 1));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 2));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void destroy() {
        new File(dbPath.toString()).delete();
    }

    private OrganizedFileIndexStorageManager getCompactFileIndexStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new OrganizedFileIndexStorageManager(
                "test",
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
    public void testMultiSplitAddIndex() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager2 = new ClusterBPlusTreeUniqueTreeIndexManager<>(2, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        for (int tableId = 1; tableId <= 2; tableId++){
            UniqueTreeIndexManager<Long, Pointer> currentUniqueTreeIndexManager = null;
            if (tableId == 1)
                currentUniqueTreeIndexManager = uniqueTreeIndexManager;
            else
                currentUniqueTreeIndexManager = uniqueTreeIndexManager2;

            for (long testIdentifier : testIdentifiers) {
                currentUniqueTreeIndexManager.addIndex(testIdentifier, samplePointer);
            }

            Optional<IndexStorageManager.NodeData> optional = organizedFileIndexStorageManager.getRoot(tableId, new KVSize(DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size(), PointerIndexBinaryObject.BYTES)).get();
            Assertions.assertTrue(optional.isPresent());
            Assertions.assertTrue(optional.get().pointer().getChunk() != 0);

            StoredTreeStructureVerifier.testOrderedTreeStructure(organizedFileIndexStorageManager, tableId, 1, degree);

        }

    }

    @Test
    public void testMultiSplitAddIndexLimitedOpenFiles_SuccessLimitTo1() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        

        LimitedFileHandlerPool limitedFileHandlerPool = new LimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance(), 1);
        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager1 = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager2 = new ClusterBPlusTreeUniqueTreeIndexManager<>(2, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        for (int tableId = 1; tableId <= 2; tableId++) {
            UniqueTreeIndexManager<Long, Pointer> currentUniqueTreeIndexManager = null;
            if (tableId == 1)
                currentUniqueTreeIndexManager = uniqueTreeIndexManager1;
            else
                currentUniqueTreeIndexManager = uniqueTreeIndexManager2;

            for (long testIdentifier : testIdentifiers) {
                currentUniqueTreeIndexManager.addIndex(testIdentifier, samplePointer);
            }
        }

        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        FileHandler fileHandler = limitedFileHandlerPool.getFileHandler(indexPath.toString());
        Assertions.assertNull(fileHandler);

        indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 1));
        fileHandler = limitedFileHandlerPool.getFileHandler(indexPath.toString());
        Assertions.assertNull(fileHandler);
    }

    @Test
    public void testMultiSplitAddIndexLimitedOpenFiles_SuccessLimitTo2() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);


        LimitedFileHandlerPool limitedFileHandlerPool = new LimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance(), 2);
        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager1 = new ClusterBPlusTreeUniqueTreeIndexManager(1, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager2 = new ClusterBPlusTreeUniqueTreeIndexManager(2, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        for (int tableId = 1; tableId <= 2; tableId++) {
            UniqueTreeIndexManager<Long, Pointer> currentUniqueTreeIndexManager = null;
            if (tableId == 1)
                currentUniqueTreeIndexManager = uniqueTreeIndexManager1;
            else
                currentUniqueTreeIndexManager = uniqueTreeIndexManager2;

            for (long testIdentifier : testIdentifiers) {
                currentUniqueTreeIndexManager.addIndex(testIdentifier, samplePointer);
            }
        }


        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        FileHandler fileHandler = limitedFileHandlerPool.getFileHandler(indexPath.toString());
        Assertions.assertNull(fileHandler);

        indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 1));
        fileHandler = limitedFileHandlerPool.getFileHandler(indexPath.toString());
        Assertions.assertNull(fileHandler);

    }


    @Test
    public void testMultiSplitAddIndexDifferentAddOrders() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager1 = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager2 = new ClusterBPlusTreeUniqueTreeIndexManager<>(2, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        int index = 0;
        int runs = 0;
        while (runs < testIdentifiers.size()){
            uniqueTreeIndexManager1.addIndex(testIdentifiers.get(index), samplePointer);
            uniqueTreeIndexManager2.addIndex(testIdentifiers.get(index) * 10, samplePointer);
            index++;
            runs++;
        }


        for (int tableId = 1; tableId <= 2; tableId++) {

            int multi = 1;
            if (tableId == 2){
                multi = 10;
            }

            StoredTreeStructureVerifier.testOrderedTreeStructure(organizedFileIndexStorageManager, tableId, multi, degree);

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
    public void testMultiSplitAddIndex2() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 4L, 9L, 6L, 10L, 8L, 3L, 2L, 11L, 5L, 7L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager1 = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager2 = new ClusterBPlusTreeUniqueTreeIndexManager<>(2, degree, organizedFileIndexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());

        for (int tableId = 1; tableId <= 2; tableId++){
            UniqueTreeIndexManager<Long, Pointer> currentUniqueTreeIndexManager = null;
            if (tableId == 1)
                currentUniqueTreeIndexManager = uniqueTreeIndexManager1;
            else
                currentUniqueTreeIndexManager = uniqueTreeIndexManager2;

            for (long testIdentifier : testIdentifiers) {
                currentUniqueTreeIndexManager.addIndex(testIdentifier, samplePointer);
            }

            StoredTreeStructureVerifier.testUnOrderedTreeStructure1(organizedFileIndexStorageManager, tableId, 1, degree);

        }

    }

}
