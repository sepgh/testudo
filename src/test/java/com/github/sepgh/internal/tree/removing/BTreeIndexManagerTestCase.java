package com.github.sepgh.internal.tree.removing;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.storage.FileIndexStorageManager;
import com.github.sepgh.internal.storage.InMemoryHeaderManager;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.tree.BTreeIndexManager;
import com.github.sepgh.internal.tree.IndexManager;
import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.storage.FileIndexStorageManager.INDEX_FILE_NAME;

public class BTreeIndexManagerTestCase {

    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BTreeIndexManagerReadingTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeNodeMaxKey(degree - 1)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(12L * engineConfig.getPaddedSize());

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


    @Test
    public void testRemoving() throws IOException, ExecutionException, InterruptedException {
        List<Long> testIdentifiers = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L);
        Pointer samplePointer = new Pointer(Pointer.TYPE_DATA, 100, 0);


        HeaderManager headerManager = new InMemoryHeaderManager(header);
        FileIndexStorageManager fileIndexStorageManager = new FileIndexStorageManager(dbPath, headerManager, engineConfig);
        IndexManager indexManager = new BTreeIndexManager(degree, fileIndexStorageManager);


        for (Long testIdentifier : testIdentifiers) {
            indexManager.addIndex(1, testIdentifier, samplePointer);
        }

//        Assertions.assertFalse(indexManager.removeIndex(1, 13));
//        Assertions.assertTrue(indexManager.removeIndex(1, 12));
//        Assertions.assertFalse(indexManager.removeIndex(1, 12));
        Assertions.assertTrue(indexManager.removeIndex(1, 11));
//        Assertions.assertFalse(indexManager.removeIndex(1, 11));

    }

}
