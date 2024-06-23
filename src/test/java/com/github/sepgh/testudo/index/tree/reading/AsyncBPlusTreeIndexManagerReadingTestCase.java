package com.github.sepgh.testudo.index.tree.reading;

import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.TableLevelAsyncIndexManagerDecorator;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.CompactFileIndexStorageManager;
import com.github.sepgh.testudo.storage.InMemoryHeaderManager;
import com.github.sepgh.testudo.storage.header.Header;
import com.github.sepgh.testudo.storage.header.HeaderManager;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.sepgh.testudo.storage.CompactFileIndexStorageManager.INDEX_FILE_NAME;

public class AsyncBPlusTreeIndexManagerReadingTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private Header header;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BTreeIndexManagerReadingTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(12L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

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
    @Timeout(value = 2)
    public void findIndexSuccessfully() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        IndexManager<Long, Pointer> indexManager = new TableLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new LongImmutableBinaryObjectWrapper()));
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        indexManager.addIndex(1, 10L, dataPointer);
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 10L);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());
    }

    @Test
    @Timeout(value = 2)
    public void getTableSize() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        IndexManager<Long, Pointer> indexManager = new TableLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new LongImmutableBinaryObjectWrapper()));
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        CountDownLatch countDownLatch = new CountDownLatch(100);
        for(long i = 1; i <= 100; i++){
            long finalI = i;
            executorService.submit(() -> {
                try {
                    indexManager.addIndex(1, finalI, dataPointer);
                } catch (ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue | IndexExistsException |
                         InternalOperationException e) {
                    throw new RuntimeException(e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }

        countDownLatch.await();
        Assertions.assertEquals(100, indexManager.size(1));
    }

    @Test
    @Timeout(value = 2)
    public void findIndexFailure() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        HeaderManager headerManager = new InMemoryHeaderManager(header);
        CompactFileIndexStorageManager compactFileIndexStorageManager = new CompactFileIndexStorageManager(dbPath, headerManager, engineConfig, BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        IndexManager<Long, Pointer> indexManager = new TableLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, compactFileIndexStorageManager, new LongImmutableBinaryObjectWrapper()));
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        indexManager.addIndex(1, 10L, dataPointer);
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 100L);

        Assertions.assertFalse(optionalPointer.isPresent());
    }
}
