package com.github.sepgh.test.index.tree.storing;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.CompactFileIndexStorageManager;
import com.github.sepgh.testudo.storage.IndexStorageManager;
import com.github.sepgh.testudo.storage.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.testudo.storage.CompactFileIndexStorageManager.INDEX_FILE_NAME;

/*
*  The purpose of this test case is to assure allocation wouldn't cause issue in multi-table environment
*/
public class MultiTableBPlusTreeIndexManagerCompactAllocationTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_MultiTableBTreeIndexManagerAllocationTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(15L * 2 * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        byte[] writingBytes = new byte[6 * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES)];
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
                dbPath,
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
    public void testMultiSplitAddIndex() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        CompactFileIndexStorageManager compactFileIndexStorageManager = getCompactFileIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());


        for (int tableId = 1; tableId <= 2; tableId++){

            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }

            Optional<IndexStorageManager.NodeData> optional = compactFileIndexStorageManager.getRoot(tableId).get();
            Assertions.assertTrue(optional.isPresent());

            StoredTreeStructureVerifier.testOrderedTreeStructure(compactFileIndexStorageManager, tableId, 1, degree);
        }

    }


    @Test
    public void testMultiSplitAddIndexDifferentAddOrders() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        CompactFileIndexStorageManager compactFileIndexStorageManager = getCompactFileIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());

        int index = 0;
        int runs = 0;
        while (runs < testIdentifiers.size()){
            indexManager.addIndex(1, testIdentifiers.get(index), samplePointer);
            indexManager.addIndex(2, testIdentifiers.get(index) * 10, samplePointer);
            index++;
            runs++;
        }


        for (int tableId = 1; tableId <= 2; tableId++) {

            int multi = 1;
            if (tableId == 2){
                multi = 10;
            }

            Optional<IndexStorageManager.NodeData> optional = compactFileIndexStorageManager.getRoot(tableId).get();
            Assertions.assertTrue(optional.isPresent());

            StoredTreeStructureVerifier.testOrderedTreeStructure(compactFileIndexStorageManager, tableId, multi, degree);
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


        CompactFileIndexStorageManager compactFileIndexStorageManager = getCompactFileIndexStorageManager();
        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new LongImmutableBinaryObjectWrapper());

        for (int tableId = 1; tableId <= 2; tableId++){

            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }

            Optional<IndexStorageManager.NodeData> optional = compactFileIndexStorageManager.getRoot(tableId).get();
            Assertions.assertTrue(optional.isPresent());

            StoredTreeStructureVerifier.testUnOrderedTreeStructure1(compactFileIndexStorageManager, tableId, 1, degree);
        }

    }

}
