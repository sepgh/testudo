package com.github.sepgh.test.index.tree.reading;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.CompactFileIndexStorageManager;
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

import static com.github.sepgh.testudo.storage.index.CompactFileIndexStorageManager.INDEX_FILE_NAME;

public class BPlusTreeIndexManagerReadingTestCase {
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
        engineConfig.setBTreeMaxFileSize(12L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongImmutableBinaryObjectWrapper.BYTES));

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    private CompactFileIndexStorageManager getStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new CompactFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.Factory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    @Test
    @Timeout(value = 2)
    public void findIndexSuccessfully() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        CompactFileIndexStorageManager indexStorageManager = getStorageManager();

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        indexManager.addIndex(1, 10L, dataPointer);
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 10L);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());
    }

    @Test
    @Timeout(value = 2)
    public void getTableSize() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        CompactFileIndexStorageManager indexStorageManager = getStorageManager();

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for(long i = 1; i <= 100; i++)
            indexManager.addIndex(1, i, dataPointer);

        Assertions.assertEquals(100, indexManager.size(1));

        indexStorageManager.close();
    }


    @Test
    @Timeout(value = 2)
    public void findIndexFailure() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        CompactFileIndexStorageManager indexStorageManager = getStorageManager();

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        indexManager.addIndex(1, 10L, dataPointer);
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 100L);

        Assertions.assertFalse(optionalPointer.isPresent());

        indexStorageManager.close();
    }
    @Test
    @Timeout(value = 2)
    public void readAndWriteZero() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, InternalOperationException, IndexExistsException {
        CompactFileIndexStorageManager indexStorageManager = getStorageManager();

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, new LongImmutableBinaryObjectWrapper());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        indexManager.addIndex(1, 0L, dataPointer);
        Optional<Pointer> optionalPointer = indexManager.getIndex(1, 0L);
        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        indexStorageManager.close();
    }
}
