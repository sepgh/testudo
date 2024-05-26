package com.github.sepgh.internal.index.tree.storing;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.helper.IndexFileDescriptor;
import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.BaseTreeNode;
import com.github.sepgh.internal.index.tree.node.LeafTreeNode;
import com.github.sepgh.internal.storage.FileIndexStorageManager;
import com.github.sepgh.internal.storage.InMemoryHeaderManager;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.index.tree.BPlusTreeIndexManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.storage.FileIndexStorageManager.INDEX_FILE_NAME;

public class MultiTableBPlusTreeIndexManagerTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_MultiTableBTreeIndexManagerTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(10)
                .build();
        engineConfig.setBTreeMaxFileSize(2 * 15L * engineConfig.getPaddedSize());

        byte[] writingBytes = new byte[2 * 13 * engineConfig.getPaddedSize()];
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
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
                                                                .offset(12L * engineConfig.getPaddedSize())
                                                                .build()
                                                )
                                        )
                                        .root(
                                                Header.IndexChunk.builder()
                                                        .chunk(0)
                                                        .offset(12L * engineConfig.getPaddedSize())
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
    public void destroy() throws IOException {
        Path indexPath0 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.delete(indexPath0);
        try {
            Path indexPath1 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
            Files.delete(indexPath1);
        } catch (NoSuchFileException ignored){}
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
    public void testMultiSplitAddIndex() throws IOException, ExecutionException, InterruptedException {

        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (int tableId = 1; tableId <= 2; tableId++){
            HeaderManager headerManager = new InMemoryHeaderManager(header);
            FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);
            IndexManager indexManager = new BPlusTreeIndexManager(degree, fileIndexStorageManager);


            BaseTreeNode lastTreeNode = null;
            for (long testIdentifier : testIdentifiers) {
                lastTreeNode = indexManager.addIndex(tableId, testIdentifier, samplePointer);
            }

            Assertions.assertTrue(lastTreeNode.isLeaf());
            Assertions.assertEquals(2, lastTreeNode.getKeyList(degree).size());
            Assertions.assertEquals(samplePointer.getPosition(), ((LeafTreeNode) lastTreeNode).getKeyValues(degree).next().value().getPosition());

            StoredTreeStructureVerifier.testOrderedTreeStructure(fileIndexStorageManager, tableId, 1, degree);
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
        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);
        IndexManager indexManager = new BPlusTreeIndexManager(degree, fileIndexStorageManager);

        IndexFileDescriptor indexFileDescriptor = new IndexFileDescriptor(
                AsynchronousFileChannel.open(
                        Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0)),
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE
                ),
                headerManager,
                engineConfig
        );

        for (int tableId = 1; tableId <= 2; tableId++){

            BaseTreeNode lastTreeNode = null;
            for (long testIdentifier : testIdentifiers) {
                lastTreeNode = indexManager.addIndex(tableId, testIdentifier, samplePointer);
                System.out.println("Adding " + testIdentifier + " to table " + tableId);
                indexFileDescriptor.describe();
            }

            Assertions.assertTrue(lastTreeNode.isLeaf());
            Assertions.assertEquals(2, lastTreeNode.getKeyList(degree).size());
            Assertions.assertEquals(samplePointer.getPosition(), ((LeafTreeNode) lastTreeNode).getKeyValues(degree).next().value().getPosition());

            StoredTreeStructureVerifier.testUnOrderedTreeStructure1(fileIndexStorageManager, tableId, 1, degree);

        }

    }

}
