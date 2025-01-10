package com.github.sepgh.test.index;

import com.github.sepgh.test.TestParams;
import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.DuplicateBPlusTreeIndexManagerBridge;
import com.github.sepgh.testudo.index.DuplicateBitmapIndexManager;
import com.github.sepgh.testudo.index.DuplicateQueryableIndex;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.data.PointerIndexBinaryObject;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.serialization.IntegerSerializer;
import com.github.sepgh.testudo.storage.db.DiskPageDatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

public class DuplicateQueryableIndexTestCase {

    private Path dbPath;
    private EngineConfig engineConfig;
    private DiskPageDatabaseStorageManager diskPageDatabaseStorageManager;
    

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory("TEST_DuplicateQueryableIndexTestCase");
        this.engineConfig = EngineConfig.builder()
                .baseDBPath(this.dbPath.toString())
                .bTreeDegree(10)
                .build();

        this.diskPageDatabaseStorageManager = new DiskPageDatabaseStorageManager(
                engineConfig,
                new UnlimitedFileHandlerPool(
                        FileHandler.SingletonFileHandlerFactory.getInstance()
                )
        );
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    private UniqueQueryableIndex<Integer, Pointer> getIntegerPointerUniqueTreeIndexManager() {
        IndexStorageManager indexStorageManager = new OrganizedFileIndexStorageManager(new JsonIndexHeaderManager.SingletonFactory(), engineConfig, new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance()));

        return new BPlusTreeUniqueTreeIndexManager<>(
                1,
                engineConfig.getBTreeDegree(),
                indexStorageManager,
                new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD),
                new PointerIndexBinaryObject.Factory()
        );
    }
    
    @Test
    @Timeout(value = 2)
    public void testGreaterThan_Bitmap() throws IOException, ExecutionException, InterruptedException, InternalOperationException, DeserializationException {
        UniqueQueryableIndex<Integer, Pointer> uniqueTreeIndexManager = getIntegerPointerUniqueTreeIndexManager();

        DuplicateQueryableIndex<Integer, Integer> duplicateQueryableIndex = new DuplicateBitmapIndexManager<>(
                1,
                uniqueTreeIndexManager,
                new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD),
                diskPageDatabaseStorageManager
        );

        runLargerThanTest(duplicateQueryableIndex);
    }


    @Test
    @Timeout(value = 2)
    public void testLessThan_Bitmap() throws IOException, ExecutionException, InterruptedException, InternalOperationException, DeserializationException {
        UniqueQueryableIndex<Integer, Pointer> uniqueTreeIndexManager = getIntegerPointerUniqueTreeIndexManager();

        DuplicateQueryableIndex<Integer, Integer> duplicateQueryableIndex = new DuplicateBitmapIndexManager<>(
                1,
                uniqueTreeIndexManager,
                new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD),
                diskPageDatabaseStorageManager
        );

        runLessThanTest(duplicateQueryableIndex);

    }

    @Test
    @Timeout(value = 2)
    public void testGreaterThan_BPlusTreeBridge() throws IOException, ExecutionException, InterruptedException, InternalOperationException, DeserializationException {
        UniqueQueryableIndex<Integer, Pointer> uniqueTreeIndexManager = getIntegerPointerUniqueTreeIndexManager();

        DuplicateQueryableIndex<Integer, Integer> duplicateQueryableIndex = new DuplicateBPlusTreeIndexManagerBridge<>(
                1,
                engineConfig,
                uniqueTreeIndexManager,
                new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD),
                diskPageDatabaseStorageManager
        );

        runLargerThanTest(duplicateQueryableIndex);
    }


    @Test
    @Timeout(value = 2)
    public void testLessThan_BPlusTreeBridge() throws IOException, ExecutionException, InterruptedException, InternalOperationException, DeserializationException {
        UniqueQueryableIndex<Integer, Pointer> uniqueTreeIndexManager = getIntegerPointerUniqueTreeIndexManager();

        DuplicateQueryableIndex<Integer, Integer> duplicateQueryableIndex = new DuplicateBPlusTreeIndexManagerBridge<>(
                1,
                engineConfig,
                uniqueTreeIndexManager,
                new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD),
                diskPageDatabaseStorageManager
        );

        runLessThanTest(duplicateQueryableIndex);

    }


    private void runLargerThanTest(DuplicateQueryableIndex<Integer, Integer> duplicateQueryableIndex) throws InternalOperationException, IOException, ExecutionException, InterruptedException, DeserializationException {
        for (int i = 1; i < 4; i++) {
            for (int j = 0; j < 3; j++) {
                int add = (int) (i * Math.pow(10, j));
                duplicateQueryableIndex.addIndex(i, add);
            }
        }


        Iterator<Integer> greaterThan = duplicateQueryableIndex.getGreaterThan(1, Order.ASC);
        for (int i = 2; i < 4; i++) {
            for (int j = 0; j < 3; j++) {
                int expectedNext = (int) (i * Math.pow(10, j));
                Assertions.assertTrue(greaterThan.hasNext(), "Expected to have next for " + expectedNext);
                Assertions.assertEquals(expectedNext, greaterThan.next());
            }
        }
        Assertions.assertFalse(greaterThan.hasNext());


        Iterator<Integer> greaterThanEQ = duplicateQueryableIndex.getGreaterThanEqual(1, Order.ASC);
        for (int i = 1; i < 4; i++) {
            for (int j = 0; j < 3; j++) {
                int expectedNext = (int) (i * Math.pow(10, j));
                Assertions.assertTrue(greaterThanEQ.hasNext(), "Expected to have next for " + expectedNext);
                Assertions.assertEquals(expectedNext, greaterThanEQ.next());
            }
        }
        Assertions.assertFalse(greaterThan.hasNext());


        greaterThan = duplicateQueryableIndex.getGreaterThan(1, Order.DESC);
        for (int i = 3; i > 1; i--) {
            for (int j = 2; j >= 0; j--) {
                int expectedNext = (int) (i * Math.pow(10, j));
                Assertions.assertTrue(greaterThan.hasNext(), "Expected to have next for " + expectedNext);
                Assertions.assertEquals(expectedNext, greaterThan.next());
            }
        }
        Assertions.assertFalse(greaterThan.hasNext());


        greaterThanEQ = duplicateQueryableIndex.getGreaterThanEqual(1, Order.DESC);
        for (int i = 3; i >= 1; i--) {
            for (int j = 2; j >= 0; j--) {
                int expectedNext = (int) (i * Math.pow(10, j));
                Assertions.assertTrue(greaterThanEQ.hasNext(), "Expected to have next for " + expectedNext);
                Assertions.assertEquals(expectedNext, greaterThanEQ.next());
            }
        }
        Assertions.assertFalse(greaterThan.hasNext());

    }

    private void runLessThanTest(DuplicateQueryableIndex<Integer, Integer> duplicateQueryableIndex) throws InternalOperationException, IOException, ExecutionException, InterruptedException, DeserializationException {
        for (int i = 1; i < 4; i++) {
            for (int j = 0; j < 3; j++) {
                int add = (int) (i * Math.pow(10, j));
                duplicateQueryableIndex.addIndex(i, add);
            }
        }


        Iterator<Integer> lessThan = duplicateQueryableIndex.getLessThan(3, Order.DESC);
        for (int i = 2; i >= 1; i--) {
            for (int j = 2; j >= 0; j--) {
                int expectedNext = (int) (i * Math.pow(10, j));
                Assertions.assertTrue(lessThan.hasNext(), "Expected to have next for " + expectedNext);
                Assertions.assertEquals(expectedNext, lessThan.next());
            }
        }
        Assertions.assertFalse(lessThan.hasNext());


        Iterator<Integer> lessThanEQ = duplicateQueryableIndex.getLessThanEqual(3, Order.DESC);
        for (int i = 3; i >= 1; i--) {
            for (int j = 2; j >= 0; j--) {
                int expectedNext = (int) (i * Math.pow(10, j));
                Assertions.assertTrue(lessThanEQ.hasNext(), "Expected to have next for " + expectedNext);
                Assertions.assertEquals(expectedNext, lessThanEQ.next());
            }
        }
        Assertions.assertFalse(lessThanEQ.hasNext());



        lessThan = duplicateQueryableIndex.getLessThan(3, Order.ASC);
        for (int i = 1; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                int expectedNext = (int) (i * Math.pow(10, j));
                Assertions.assertTrue(lessThan.hasNext(), "Expected to have next for " + expectedNext);
                Assertions.assertEquals(expectedNext, lessThan.next());
            }
        }
        Assertions.assertFalse(lessThan.hasNext());


        lessThanEQ = duplicateQueryableIndex.getLessThanEqual(3, Order.ASC);
        for (int i = 1; i <= 3; i++) {
            for (int j = 0; j < 3; j++) {
                int expectedNext = (int) (i * Math.pow(10, j));
                Assertions.assertTrue(lessThanEQ.hasNext(), "Expected to have next for " + expectedNext);
                Assertions.assertEquals(expectedNext, lessThanEQ.next());
            }
        }
        Assertions.assertFalse(lessThanEQ.hasNext());
    }


}
