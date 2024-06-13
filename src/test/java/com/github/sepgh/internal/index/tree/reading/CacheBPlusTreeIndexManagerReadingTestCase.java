package com.github.sepgh.internal.index.tree.reading;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.index.CachedIndexManagerDecorator;
import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.TableLevelAsyncIndexManagerDecorator;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.internal.index.tree.node.data.BinaryObjectWrapper;
import com.github.sepgh.internal.index.tree.node.data.LongBinaryObjectWrapper;
import com.github.sepgh.internal.storage.*;
import com.github.sepgh.internal.storage.header.Header;
import com.github.sepgh.internal.storage.header.HeaderManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.internal.storage.CompactFileIndexStorageManager.INDEX_FILE_NAME;

public class CacheBPlusTreeIndexManagerReadingTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private int degree = 4;
    private Path indexPath;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_CacheBPlusTreeIndexManagerReadingTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(12L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongBinaryObjectWrapper.BYTES));

        byte[] writingBytes = new byte[]{};
        indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
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

    public boolean destroy() throws IOException {
        return new File(indexPath.toString()).delete();
    }

    @Test
    @Timeout(value = 2)
    public void findIndexSuccessfully_cachedStorage() throws IOException, ExecutionException, InterruptedException, BinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        IndexStorageManager indexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongBinaryObjectWrapper.BYTES));
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 30);

        IndexManager<Long, Pointer> indexManager = new TableLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongBinaryObjectWrapper()));
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 0; i < 20; i++){
            indexManager.addIndex(1, i, dataPointer);
        }
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 10L);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        Assertions.assertTrue(destroy());
        optionalPointer = indexManager.getIndex(1, 10L);
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        indexStorageManager.close();

        indexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongBinaryObjectWrapper.BYTES));
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 12);

        indexManager = new TableLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongBinaryObjectWrapper()));
        IndexManager<Long, Pointer> finalIndexManager = indexManager;
        Assertions.assertThrows(IOException.class, () -> {
            finalIndexManager.getIndex(1, 10L);
        }, "Nothing available to read.");
    }

    @Test
    @Timeout(value = 2)
    public void findIndexFailure_cachedStorage() throws IOException, ExecutionException, InterruptedException, BinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        IndexStorageManager indexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongBinaryObjectWrapper.BYTES));
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 12);

        IndexManager<Long, Pointer> indexManager = new TableLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongBinaryObjectWrapper()));
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 0; i < 20; i++){
            indexManager.addIndex(1, i, dataPointer);
        }

        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 100L);
        Assertions.assertFalse(optionalPointer.isPresent());

        // Removing file and checking if we can still find index
        Assertions.assertTrue(destroy());
        optionalPointer = indexManager.getIndex(1, 100L);
        Assertions.assertFalse(optionalPointer.isPresent());
    }

    @Test
    @Timeout(value = 2)
    public void findIndexSuccessfully_cachedIndexManager() throws IOException, ExecutionException, InterruptedException, BinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        IndexStorageManager indexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongBinaryObjectWrapper.BYTES));

        IndexManager<Long, Pointer> indexManager = new CachedIndexManagerDecorator<>(
                new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongBinaryObjectWrapper()),
                20
        );
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 1; i < 20; i++){
            indexManager.addIndex(1, i, dataPointer);
        }
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 10L);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        Assertions.assertTrue(destroy());
        optionalPointer = indexManager.getIndex(1, 10L);
        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());
    }

    @Test
    @Timeout(value = 2)
    public void findIndexFailure_cachedIndexManager() throws IOException, ExecutionException, InterruptedException, BinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        IndexStorageManager indexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongBinaryObjectWrapper.BYTES));
        IndexManager<Long, Pointer> indexManager = new CachedIndexManagerDecorator<>(
                new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongBinaryObjectWrapper()),
                20
        );
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 1; i < 20; i++){
            indexManager.addIndex(1, i, dataPointer);
        }

        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 100L);
        Assertions.assertFalse(optionalPointer.isPresent());

        Assertions.assertTrue(destroy());
        optionalPointer = indexManager.getIndex(1, 100L);
        Assertions.assertFalse(optionalPointer.isPresent());
    }
}
