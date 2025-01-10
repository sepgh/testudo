package com.github.sepgh.test.index.tree.reading;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.CachedUniqueQueryableIndexDecorator;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.CachedIndexStorageManagerDecorator;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
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

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;
import static com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager.INDEX_FILE_NAME;

public class CacheBPlusTreeUniqueTreeIndexManagerReadingTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private int degree = 4;
    private Path indexPath;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_CacheBPlusTreeIndexManagerReadingTestCase");
        engineConfig = EngineConfig.builder()
                .baseDBPath(dbPath.toString())
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(12L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size()));

        byte[] writingBytes = new byte[]{};
        indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    private OrganizedFileIndexStorageManager getStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new OrganizedFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.SingletonFactory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    @Test
    @Timeout(value = 2)
    public void findIndexSuccessfully_cachedStorage() throws IOException, ExecutionException, InterruptedException, InternalOperationException, IndexExistsException {
        IndexStorageManager indexStorageManager = getStorageManager();
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 30);

        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 0; i < 20; i++){
            uniqueTreeIndexManager.addIndex(i, dataPointer);
        }
        Optional<Pointer> optionalPointer = uniqueTreeIndexManager.getIndex(10L);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        optionalPointer = uniqueTreeIndexManager.getIndex(10L);
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        indexStorageManager.close();
        destroy();
        setUp();

        indexStorageManager = getStorageManager();
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 12);

        uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        UniqueTreeIndexManager<Long, Pointer> finalUniqueTreeIndexManager = uniqueTreeIndexManager;

        Optional<Pointer> index = finalUniqueTreeIndexManager.getIndex(10L);
        Assertions.assertTrue(index.isEmpty());  // Well, we removed root from header, so we get empty optional there already

        indexStorageManager.close();
    }

    @Test
    @Timeout(value = 2)
    public void findIndexFailure_cachedStorage() throws IOException, ExecutionException, InterruptedException, InternalOperationException, IndexExistsException {
        IndexStorageManager indexStorageManager = getStorageManager();
        indexStorageManager = new CachedIndexStorageManagerDecorator(indexStorageManager, 12);

        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 0; i < 20; i++){
            uniqueTreeIndexManager.addIndex(i, dataPointer);
        }

        Optional<Pointer> optionalPointer = uniqueTreeIndexManager.getIndex(100L);
        Assertions.assertFalse(optionalPointer.isPresent());

        // Removing file and checking if we can still find index
        destroy();
        optionalPointer = uniqueTreeIndexManager.getIndex(100L);
        Assertions.assertFalse(optionalPointer.isPresent());

        indexStorageManager.close();
    }

    @Test
    @Timeout(value = 2)
    public void findIndexSuccessfully_cachedIndexManager() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        IndexStorageManager indexStorageManager = getStorageManager();

        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new CachedUniqueQueryableIndexDecorator<>(
                new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get()),
                20
        );
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 1; i < 20; i++){
            uniqueTreeIndexManager.addIndex(i, dataPointer);
        }
        Optional<Pointer> optionalPointer = uniqueTreeIndexManager.getIndex(10L);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        destroy();
        optionalPointer = uniqueTreeIndexManager.getIndex(10L);
        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        indexStorageManager.close();
    }

    @Test
    @Timeout(value = 2)
    public void findIndexFailure_cachedIndexManager() throws IOException, ExecutionException, InterruptedException, InternalOperationException, IndexExistsException {
        IndexStorageManager indexStorageManager = getStorageManager();
        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new CachedUniqueQueryableIndexDecorator<>(
                new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get()),
                20
        );
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for (long i = 1; i < 20; i++){
            uniqueTreeIndexManager.addIndex(i, dataPointer);
        }

        Optional<Pointer> optionalPointer = uniqueTreeIndexManager.getIndex(100L);
        Assertions.assertFalse(optionalPointer.isPresent());

        destroy();
        optionalPointer = uniqueTreeIndexManager.getIndex(100L);
        Assertions.assertFalse(optionalPointer.isPresent());

        indexStorageManager.close();
    }
}
