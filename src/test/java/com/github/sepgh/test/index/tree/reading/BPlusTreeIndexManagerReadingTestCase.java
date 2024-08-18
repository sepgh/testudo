package com.github.sepgh.test.index.tree.reading;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.IndexBinaryObject;
import com.github.sepgh.testudo.index.tree.node.data.LongIndexBinaryObject;
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

import static com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager.INDEX_FILE_NAME;

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
        engineConfig.setBTreeMaxFileSize(12L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, LongIndexBinaryObject.BYTES));

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
    public void findIndexSuccessfully() throws IOException, ExecutionException, InterruptedException, IndexBinaryObject.InvalidIndexBinaryObject, InternalOperationException, IndexExistsException {
        OrganizedFileIndexStorageManager indexStorageManager = getStorageManager();

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(1, degree, indexStorageManager, new LongIndexBinaryObject.Factory());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        indexManager.addIndex(10L, dataPointer);
        Optional<Pointer> optionalPointer = indexManager.getIndex(10L);

        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());
    }

    @Test
    @Timeout(value = 2)
    public void getTableSize() throws IOException, ExecutionException, InterruptedException, IndexBinaryObject.InvalidIndexBinaryObject, InternalOperationException, IndexExistsException {
        OrganizedFileIndexStorageManager indexStorageManager = getStorageManager();

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(1, degree, indexStorageManager, new LongIndexBinaryObject.Factory());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        for(long i = 1; i <= 100; i++)
            indexManager.addIndex(i, dataPointer);

        Assertions.assertEquals(100, indexManager.size());

        indexStorageManager.close();
    }

    @Test
    @Timeout(value = 2)
    public void findIndexFailure() throws IOException, ExecutionException, InterruptedException, IndexBinaryObject.InvalidIndexBinaryObject, InternalOperationException, IndexExistsException {
        OrganizedFileIndexStorageManager indexStorageManager = getStorageManager();

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(1, degree, indexStorageManager, new LongIndexBinaryObject.Factory());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        indexManager.addIndex(10L, dataPointer);
        Optional<Pointer> optionalPointer = indexManager.getIndex( 100L);

        Assertions.assertFalse(optionalPointer.isPresent());

        indexStorageManager.close();
    }

    @Test
    @Timeout(value = 2)
    public void readAndWriteZero() throws IOException, ExecutionException, InterruptedException, IndexBinaryObject.InvalidIndexBinaryObject, InternalOperationException, IndexExistsException {
        OrganizedFileIndexStorageManager indexStorageManager = getStorageManager();

        IndexManager<Long, Pointer> indexManager = new ClusterBPlusTreeIndexManager<>(1, degree, indexStorageManager, new LongIndexBinaryObject.Factory());
        Pointer dataPointer = new Pointer(Pointer.TYPE_DATA, 100, 0);

        indexManager.addIndex(0L, dataPointer);
        Optional<Pointer> optionalPointer = indexManager.getIndex(0L);
        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(dataPointer, optionalPointer.get());

        indexStorageManager.close();
    }
}
