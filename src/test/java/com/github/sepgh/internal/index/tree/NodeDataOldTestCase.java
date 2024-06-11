package com.github.sepgh.internal.index.tree;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.internal.index.tree.node.data.BinaryObjectWrapper;
import com.github.sepgh.internal.index.tree.node.data.IntegerBinaryObjectWrapper;
import com.github.sepgh.internal.index.tree.node.data.NoZeroIntegerBinaryObjectWrapper;
import com.github.sepgh.internal.index.tree.node.data.NoZeroLongBinaryObjectWrapper;
import com.github.sepgh.internal.storage.CompactFileIndexStorageManager;
import com.github.sepgh.internal.storage.InMemoryHeaderManager;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.storage.BaseFileIndexStorageManager.INDEX_FILE_NAME;

public class NodeDataOldTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_NodeInnerObjTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(4L * engineConfig.getPaddedSize());

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
    }

    @Test
    public void test_IntegerIdentifier() throws IOException, ExecutionException, InterruptedException, BinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);

        IndexManager<Integer, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new IntegerBinaryObjectWrapper());

        for (int i = 0; i < 13; i ++){
            indexManager.addIndex(1, i, Pointer.empty());
        }

        for (int i = 0; i < 13; i ++){
            Assertions.assertTrue(indexManager.getIndex(1, i).isPresent());
        }

        for (int i = 0; i < 13; i ++){
            Assertions.assertTrue(indexManager.removeIndex(1, i));
        }

        for (int i = 0; i < 13; i ++){
            Assertions.assertFalse(indexManager.getIndex(1, i).isPresent());
        }

    }
    @Test
    public void test_NoZeroIntegerIdentifier() throws IOException, ExecutionException, InterruptedException, BinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);

        IndexManager<Integer, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new NoZeroIntegerBinaryObjectWrapper());

        Assertions.assertThrows(BinaryObjectWrapper.InvalidBinaryObjectWrapperValue.class, () -> {
            indexManager.addIndex(1, 0, Pointer.empty());
        });

        for (int i = 1; i < 13; i ++){
            indexManager.addIndex(1, i, Pointer.empty());
        }

        for (int i = 1; i < 13; i ++){
            Assertions.assertTrue(indexManager.getIndex(1, i).isPresent());
        }

        for (int i = 1; i < 13; i ++){
            Assertions.assertTrue(indexManager.removeIndex(1, i));
        }

        for (int i = 1; i < 13; i ++){
            Assertions.assertFalse(indexManager.getIndex(1, i).isPresent());
        }

    }

    @Test
    public void test_NoZeroLongIdentifier() throws IOException, ExecutionException, InterruptedException, BinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new NoZeroLongBinaryObjectWrapper());

        Assertions.assertThrows(BinaryObjectWrapper.InvalidBinaryObjectWrapperValue.class, () -> {
            indexManager.addIndex(1, 0L, Pointer.empty());
        });

        for (long i = 1; i < 13; i ++){
            indexManager.addIndex(1, i, Pointer.empty());
        }

        for (long i = 1; i < 13; i ++){
            Assertions.assertTrue(indexManager.getIndex(1, i).isPresent());
        }

        for (long i = 1; i < 13; i ++){
            Assertions.assertTrue(indexManager.removeIndex(1, i));
        }

        for (long i = 1; i < 13; i ++){
            Assertions.assertFalse(indexManager.getIndex(1, i).isPresent());
        }

    }


}
