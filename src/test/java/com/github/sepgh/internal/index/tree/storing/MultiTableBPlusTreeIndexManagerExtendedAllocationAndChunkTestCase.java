package com.github.sepgh.internal.index.tree.storing;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.helper.IndexFileDescriptor;
import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.BPlusTreeIndexManager;
import com.github.sepgh.internal.storage.ExtendedFileIndexStorageManager;
import com.github.sepgh.internal.storage.InMemoryHeaderManager;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.storage.pool.FileHandler;
import com.github.sepgh.internal.storage.pool.LimitedFileHandlerPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.sepgh.internal.storage.ExtendedFileIndexStorageManager.INDEX_FILE_NAME;

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
        engineConfig.setBTreeMaxFileSize(4L * engineConfig.getPaddedSize());

        byte[] writingBytes = new byte[2 * engineConfig.getPaddedSize()];
        System.out.println(engineConfig.getPaddedSize());
        Path indexPath = Path.of(dbPath.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, 1, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        System.out.println(indexPath);

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
                                                                .offset(2L * engineConfig.getPaddedSize())
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
    public void testMultiSplitAddIndex() throws IOException, ExecutionException, InterruptedException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);
        ExtendedFileIndexStorageManager extendedFileIndexStorageManager = new ExtendedFileIndexStorageManager(dbPath, headerManager, engineConfig);
        IndexManager indexManager = new BPlusTreeIndexManager(degree, extendedFileIndexStorageManager);

        IndexFileDescriptor indexFileDescriptor = new IndexFileDescriptor(
                AsynchronousFileChannel.open(
                        Path.of(dbPath.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, 1, 0)),
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE
                ),
                headerManager,
                engineConfig
        );
        IndexFileDescriptor indexFileDescriptor2 = new IndexFileDescriptor(
                AsynchronousFileChannel.open(
                        Path.of(dbPath.toString(), String.format("%s-%d.%d", INDEX_FILE_NAME, 1, 2)),
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE
                ),
                headerManager,
                engineConfig
        );

        for (int tableId = 1; tableId <= 2; tableId++){

            for (long testIdentifier : testIdentifiers) {
                System.out.println("Adding " + testIdentifier + " to table " + tableId);
                indexManager.addIndex(tableId, testIdentifier, samplePointer);
                System.out.println("Desc 1");
                indexFileDescriptor.describe();
                System.out.println("Desc 2");
                indexFileDescriptor2.describe();
            }

            Optional<IndexStorageManager.NodeData> optional = extendedFileIndexStorageManager.getRoot(tableId).get();
            Assertions.assertTrue(optional.isPresent());
            Assertions.assertNotEquals(0, optional.get().pointer().getChunk());

            StoredTreeStructureVerifier.testOrderedTreeStructure(extendedFileIndexStorageManager, tableId, 1, degree);

        }

    }

    @Test
    public void testMultiSplitAddIndexLimitedOpenFiles_SuccessLimitTo1() throws IOException, ExecutionException, InterruptedException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);


        LimitedFileHandlerPool limitedFileHandlerPool = new LimitedFileHandlerPool(1);
        ExtendedFileIndexStorageManager extendedFileIndexStorageManager = new ExtendedFileIndexStorageManager(dbPath, headerManager, engineConfig, limitedFileHandlerPool);
        IndexManager indexManager = new BPlusTreeIndexManager(degree, extendedFileIndexStorageManager);

        for (int tableId = 1; tableId <= 2; tableId++) {
            int finalTableId = tableId;
            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(finalTableId, testIdentifier, samplePointer);
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
    public void testMultiSplitAddIndexLimitedOpenFiles_SuccessLimitTo2() throws IOException, ExecutionException, InterruptedException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);


        LimitedFileHandlerPool limitedFileHandlerPool = new LimitedFileHandlerPool(2);
        ExtendedFileIndexStorageManager extendedFileIndexStorageManager = new ExtendedFileIndexStorageManager(dbPath, headerManager, engineConfig, limitedFileHandlerPool);
        IndexManager indexManager = new BPlusTreeIndexManager(degree, extendedFileIndexStorageManager);

        for (int tableId = 1; tableId <= 2; tableId++) {
            int finalTableId = tableId;
            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(finalTableId, testIdentifier, samplePointer);
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
    public void testMultiSplitAddIndexDifferentAddOrders() throws IOException, ExecutionException, InterruptedException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);
        ExtendedFileIndexStorageManager extendedFileIndexStorageManager = new ExtendedFileIndexStorageManager(dbPath, headerManager, engineConfig);
        IndexManager indexManager = new BPlusTreeIndexManager(degree, extendedFileIndexStorageManager);

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

            StoredTreeStructureVerifier.testOrderedTreeStructure(extendedFileIndexStorageManager, tableId, multi, degree);

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
    public void testMultiSplitAddIndex2() throws IOException, ExecutionException, InterruptedException {

        List<Long> testIdentifiers = Arrays.asList(1L, 4L, 9L, 6L, 10L, 8L, 3L, 2L, 11L, 5L, 7L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        HeaderManager headerManager = new InMemoryHeaderManager(header);
        ExtendedFileIndexStorageManager extendedFileIndexStorageManager = new ExtendedFileIndexStorageManager(dbPath, headerManager, engineConfig);
        IndexManager indexManager = new BPlusTreeIndexManager(degree, extendedFileIndexStorageManager);

        for (int tableId = 1; tableId <= 2; tableId++){

            for (long testIdentifier : testIdentifiers) {
                indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }

            StoredTreeStructureVerifier.testUnOrderedTreeStructure1(extendedFileIndexStorageManager, tableId, 1, degree);

        }

    }

}
