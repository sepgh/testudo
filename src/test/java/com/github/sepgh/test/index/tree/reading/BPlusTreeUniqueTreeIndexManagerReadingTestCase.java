package com.github.sepgh.test.index.tree.reading;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;
import static com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager.INDEX_FILE_NAME;

public class BPlusTreeUniqueTreeIndexManagerReadingTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;
    private int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BTreeIndexManagerReadingTestCase");
        engineConfig = EngineConfig.builder()
                .baseDBPath(dbPath.toString())
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(12L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size()));

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    private OrganizedFileIndexStorageManager getStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new OrganizedFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    @Test
    @Timeout(value = 2)
    public void findIndexSuccessfully() throws IOException, ExecutionException, InterruptedException, InternalOperationException, IndexExistsException {
        OrganizedFileIndexStorageManager indexStorageManager = getStorageManager();

        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        uniqueTreeIndexManager.addIndex(10L, dataPointer);
        Optional<Pointer> optionalPointer = uniqueTreeIndexManager.getIndex(10L);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());
    }

    @Test
    @Timeout(value = 2)
    public void getTableSize() throws IOException, ExecutionException, InterruptedException, InternalOperationException, IndexExistsException {
        OrganizedFileIndexStorageManager indexStorageManager = getStorageManager();

        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for(long i = 1; i <= 100; i++)
            uniqueTreeIndexManager.addIndex(i, dataPointer);

        Assertions.assertEquals(100, uniqueTreeIndexManager.size());

        indexStorageManager.close();
    }

    @Test
    @Timeout(value = 2)
    public void findIndexFailure() throws IOException, ExecutionException, InterruptedException, InternalOperationException, IndexExistsException {
        OrganizedFileIndexStorageManager indexStorageManager = getStorageManager();

        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        uniqueTreeIndexManager.addIndex(10L, dataPointer);
        Optional<Pointer> optionalPointer = uniqueTreeIndexManager.getIndex( 100L);

        Assertions.assertFalse(optionalPointer.isPresent());

        indexStorageManager.close();
    }

    @Test
    @Timeout(value = 2)
    public void readAndWriteZero() throws IOException, ExecutionException, InterruptedException, InternalOperationException, IndexExistsException {
        OrganizedFileIndexStorageManager indexStorageManager = getStorageManager();

        UniqueTreeIndexManager<Long, Pointer> uniqueTreeIndexManager = new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        uniqueTreeIndexManager.addIndex(0L, dataPointer);
        Optional<Pointer> optionalPointer = uniqueTreeIndexManager.getIndex(0L);
        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        indexStorageManager.close();
    }
}
