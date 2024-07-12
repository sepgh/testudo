package com.github.sepgh.test.index.tree.reading;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.CachedIndexManagerDecorator;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.TableLevelAsyncIndexManagerDecorator;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.CachedIndexStorageManagerDecorator;
import com.github.sepgh.testudo.storage.index.CompactFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.testudo.storage.index.CompactFileIndexStorageManager.INDEX_FILE_NAME;

public class CacheBPlusTreeIndexManagerReadingTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private int degree = 4;
    private Path indexPath;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_CacheBPlusTreeIndexManagerReadingTestCase");
        engineConfig = EngineConfig.builder()
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(12L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        byte[] writingBytes = new byte[]{};
        indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    private CompactFileIndexStorageManager getStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new CompactFileIndexStorageManager(
                dbPath,
                "test",
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    @Test
    @Timeout(value = 2)
    public void findIndexSuccessfully_cachedStorage() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        IndexStorageManager indexStorageManager = getStorageManager();
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 30);

        IndexManager<Long, Pointer> indexManager = new TableLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper()));
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 0; i < 20; i++){
            indexManager.addIndex(1, i, dataPointer);
        }
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 10L);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        optionalPointer = indexManager.getIndex(1, 10L);
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        indexStorageManager.close();
        destroy();
        setUp();

        indexStorageManager = getStorageManager();
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 12);

        indexManager = new TableLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper()));
        IndexManager<Long, Pointer> finalIndexManager = indexManager;

        Optional<Pointer> index = finalIndexManager.getIndex(1, 10L);
        Assertions.assertTrue(index.isEmpty());  // Well, we removed root from header, so we get empty optional there already

        indexStorageManager.close();
    }

    @Test
    @Timeout(value = 2)
    public void findIndexFailure_cachedStorage() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        IndexStorageManager indexStorageManager = getStorageManager();
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 12);

        IndexManager<Long, Pointer> indexManager = new TableLevelAsyncIndexManagerDecorator<>(new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper()));
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 0; i < 20; i++){
            indexManager.addIndex(1, i, dataPointer);
        }

        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 100L);
        Assertions.assertFalse(optionalPointer.isPresent());

        // Removing file and checking if we can still find index
        destroy();
        optionalPointer = indexManager.getIndex(1, 100L);
        Assertions.assertFalse(optionalPointer.isPresent());

        indexStorageManager.close();
    }

    @Test
    @Timeout(value = 2)
    public void findIndexSuccessfully_cachedIndexManager() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        IndexStorageManager indexStorageManager = getStorageManager();

        IndexManager<Long, Pointer> indexManager = new CachedIndexManagerDecorator<>(
                new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper()),
                20
        );
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 1; i < 20; i++){
            indexManager.addIndex(1, i, dataPointer);
        }
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 10L);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        destroy();
        optionalPointer = indexManager.getIndex(1, 10L);
        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        indexStorageManager.close();
    }

    @Test
    @Timeout(value = 2)
    public void findIndexFailure_cachedIndexManager() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        IndexStorageManager indexStorageManager = getStorageManager();
        IndexManager<Long, Pointer> indexManager = new CachedIndexManagerDecorator<>(
                new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper()),
                20
        );
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 1; i < 20; i++){
            indexManager.addIndex(1, i, dataPointer);
        }

        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 100L);
        Assertions.assertFalse(optionalPointer.isPresent());

        destroy();
        optionalPointer = indexManager.getIndex(1, 100L);
        Assertions.assertFalse(optionalPointer.isPresent());

        indexStorageManager.close();
    }
}
