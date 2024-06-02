package com.github.sepgh.internal.index.tree.reading;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.TableLevelAsyncIndexManagerDecorator;
import com.github.sepgh.internal.index.tree.BPlusTreeIndexManager;
import com.github.sepgh.internal.storage.CachedIndexStorageManagerDecorator;
import com.github.sepgh.internal.storage.CompactFileIndexStorageManager;
import com.github.sepgh.internal.storage.InMemoryHeaderManager;
import com.github.sepgh.internal.storage.IndexStorageManager;
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
        engineConfig.setBTreeMaxFileSize(12L * engineConfig.getPaddedSize());

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
    public void findIndexSuccessfully() throws IOException, ExecutionException, InterruptedException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        IndexStorageManager indexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 12);

        IndexManager indexManager = new TableLevelAsyncIndexManagerDecorator(new BPlusTreeIndexManager(degree, indexStorageManager));
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (int i = 0; i < 20; i++){
            indexManager.addIndex(1, i, dataPointer);
        }
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 10);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        Assertions.assertTrue(destroy());
        optionalPointer = indexManager.getIndex(1, 10);
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        indexStorageManager.close();

        indexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 12);

        indexManager = new TableLevelAsyncIndexManagerDecorator(new BPlusTreeIndexManager(degree, indexStorageManager));
        optionalPointer = indexManager.getIndex(1, 10);
        System.out.println(optionalPointer);
        Assertions.assertFalse(optionalPointer.isPresent());

    }

    @Test
    @Timeout(value = 2)
    public void findIndexFailure() throws IOException, ExecutionException, InterruptedException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        IndexStorageManager indexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig);
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 12);

        IndexManager indexManager = new TableLevelAsyncIndexManagerDecorator(new BPlusTreeIndexManager(degree, indexStorageManager));
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (int i = 0; i < 20; i++){
            indexManager.addIndex(1, i, dataPointer);
        }

        // Forcing cache to be created
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 100);
        Assertions.assertFalse(optionalPointer.isPresent());

        // Removing file and checking if we can still find index
        Assertions.assertTrue(destroy());
        optionalPointer = indexManager.getIndex(1, 100);
        Assertions.assertFalse(optionalPointer.isPresent());
    }
}
