package com.github.sepgh.test.index;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.tree.BPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.storage.index.BTreeSizeCalculator;
import com.github.sepgh.testudo.storage.index.OrganizedFileIndexStorageManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;
import static com.github.sepgh.test.TestParams.LONG_INDEX_BINARY_OBJECT_FACTORY;
import static com.github.sepgh.testudo.storage.index.BaseFileIndexStorageManager.INDEX_FILE_NAME;

public class UniqueQueryableIndexTestCase {

    private Path dbPath;
    private EngineConfig engineConfig;
    private final int degree = 4;

    @BeforeEach
    public void setUp() throws IOException {
        dbPath = Files.createTempDirectory("TEST_BTreeIndexManagerTestCase");
        engineConfig = EngineConfig.builder()
                .baseDBPath(dbPath.toString())
                .bTreeDegree(degree)
                .bTreeGrowthNodeAllocationCount(2)
                .build();
        engineConfig.setBTreeMaxFileSize(4L * BTreeSizeCalculator.getClusteredBPlusTreeSize(degree, DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get().size()));

        byte[] writingBytes = new byte[]{};
        Path indexPath = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.write(indexPath, writingBytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void destroy() throws IOException {
        Path indexPath0 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
        Files.delete(indexPath0);
        try {
            Path indexPath1 = Path.of(dbPath.toString(), String.format("%s.%d", INDEX_FILE_NAME, 0));
            Files.delete(indexPath1);
        } catch (NoSuchFileException ignored){}
    }

    private OrganizedFileIndexStorageManager getCompactFileIndexStorageManager() throws IOException, ExecutionException, InterruptedException {
        return new OrganizedFileIndexStorageManager(
                "test",
                new JsonIndexHeaderManager.SingletonFactory(),
                engineConfig,
                new UnlimitedFileHandlerPool(FileHandler.SingletonFileHandlerFactory.getInstance())
        );
    }

    @Test
    @Timeout(value = 2)
    public void testGreaterThan_ASC() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();

        UniqueQueryableIndex<Long, Long> uniqueQueryableIndex = new BPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, LONG_INDEX_BINARY_OBJECT_FACTORY.get(), LONG_INDEX_BINARY_OBJECT_FACTORY.get());
        uniqueQueryableIndex.addIndex(1L, 10L);
        uniqueQueryableIndex.addIndex(2L, 20L);
        uniqueQueryableIndex.addIndex(3L, 30L);
        uniqueQueryableIndex.addIndex(4L, 40L);

        // Test Larger than 1
        Iterator<Long> largerThanIterator = uniqueQueryableIndex.getGreaterThan(1L, Order.ASC);
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(20L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(30L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(40L, largerThanIterator.next());
        Assertions.assertFalse(largerThanIterator.hasNext());

        // Test Larger than 2
        largerThanIterator = uniqueQueryableIndex.getGreaterThan(2L, Order.ASC);
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(30L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(40L, largerThanIterator.next());
        Assertions.assertFalse(largerThanIterator.hasNext());


        // Test Larger than 10
        largerThanIterator = uniqueQueryableIndex.getGreaterThan(10L, Order.ASC);
        Assertions.assertFalse(largerThanIterator.hasNext());


        // Test larger than equal 1
        Iterator<Long> largerThanEQIterator = uniqueQueryableIndex.getGreaterThanEqual(1L, Order.ASC);
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(10L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(20L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(30L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(40L, largerThanEQIterator.next());
        Assertions.assertFalse(largerThanEQIterator.hasNext());

        // Test lte 3
        largerThanEQIterator = uniqueQueryableIndex.getGreaterThanEqual(3L, Order.ASC);
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(30L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(40L, largerThanEQIterator.next());
        Assertions.assertFalse(largerThanEQIterator.hasNext());

        // Test lte 10
        largerThanEQIterator = uniqueQueryableIndex.getGreaterThanEqual(10L, Order.ASC);
        Assertions.assertFalse(largerThanEQIterator.hasNext());
    }

    @Test
    @Timeout(value = 2)
    public void testGreaterThan_DESC() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();

        UniqueQueryableIndex<Long, Long> uniqueQueryableIndex = new BPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, LONG_INDEX_BINARY_OBJECT_FACTORY.get(), LONG_INDEX_BINARY_OBJECT_FACTORY.get());
        uniqueQueryableIndex.addIndex(1L, 10L);
        uniqueQueryableIndex.addIndex(2L, 20L);
        uniqueQueryableIndex.addIndex(3L, 30L);
        uniqueQueryableIndex.addIndex(4L, 40L);

        // Test Larger than 1
        Iterator<Long> largerThanIterator = uniqueQueryableIndex.getGreaterThan(1L, Order.DESC);
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(40L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(30L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(20L, largerThanIterator.next());
        Assertions.assertFalse(largerThanIterator.hasNext());

        // Test Larger than 2
        largerThanIterator = uniqueQueryableIndex.getGreaterThan(2L, Order.DESC);
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(40L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(30L, largerThanIterator.next());
        Assertions.assertFalse(largerThanIterator.hasNext());


        // Test Larger than 10
        largerThanIterator = uniqueQueryableIndex.getGreaterThan(10L, Order.DESC);
        Assertions.assertFalse(largerThanIterator.hasNext());


        // Test larger than equal 1
        Iterator<Long> largerThanEQIterator = uniqueQueryableIndex.getGreaterThanEqual(1L, Order.DESC);
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(40L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(30L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(20L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(10L, largerThanEQIterator.next());
        Assertions.assertFalse(largerThanEQIterator.hasNext());

        // Test lte 3
        largerThanEQIterator = uniqueQueryableIndex.getGreaterThanEqual(3L, Order.DESC);
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(40L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(30L, largerThanEQIterator.next());
        Assertions.assertFalse(largerThanEQIterator.hasNext());

        // Test lte 10
        largerThanEQIterator = uniqueQueryableIndex.getGreaterThanEqual(10L, Order.DESC);
        Assertions.assertFalse(largerThanEQIterator.hasNext());
    }

    @Test
    @Timeout(value = 2)
    public void testLessThan_DESC() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();

        UniqueQueryableIndex<Long, Long> uniqueQueryableIndex = new BPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, LONG_INDEX_BINARY_OBJECT_FACTORY.get(), LONG_INDEX_BINARY_OBJECT_FACTORY.get());
        uniqueQueryableIndex.addIndex(1L, 10L);
        uniqueQueryableIndex.addIndex(2L, 20L);
        uniqueQueryableIndex.addIndex(3L, 30L);
        uniqueQueryableIndex.addIndex(4L, 40L);

        // Test Larger than 1
        Iterator<Long> largerThanIterator = uniqueQueryableIndex.getLessThan(4L, Order.DESC);
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(30L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(20L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(10L, largerThanIterator.next());
        Assertions.assertFalse(largerThanIterator.hasNext());

        // Test Larger than 2
        largerThanIterator = uniqueQueryableIndex.getLessThan(3L, Order.DESC);
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(20L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(10L, largerThanIterator.next());
        Assertions.assertFalse(largerThanIterator.hasNext());


        // Test Larger than 10
        largerThanIterator = uniqueQueryableIndex.getLessThan(1L, Order.DESC);
        Assertions.assertFalse(largerThanIterator.hasNext());


        // Test larger than equal 1
        Iterator<Long> largerThanEQIterator = uniqueQueryableIndex.getLessThanEqual(4L, Order.DESC);
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(40L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(30L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(20L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(10L, largerThanEQIterator.next());
        Assertions.assertFalse(largerThanEQIterator.hasNext());

        // Test lte 3
        largerThanEQIterator = uniqueQueryableIndex.getLessThanEqual(2L, Order.DESC);
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(20L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(10L, largerThanEQIterator.next());
        Assertions.assertFalse(largerThanEQIterator.hasNext());

        // Test lte 0
        largerThanEQIterator = uniqueQueryableIndex.getLessThanEqual(0L, Order.DESC);
        Assertions.assertFalse(largerThanEQIterator.hasNext());
    }

    @Test
    @Timeout(value = 2)
    public void testLessThan_ASC() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        OrganizedFileIndexStorageManager organizedFileIndexStorageManager = getCompactFileIndexStorageManager();

        UniqueQueryableIndex<Long, Long> uniqueQueryableIndex = new BPlusTreeUniqueTreeIndexManager<>(1, degree, organizedFileIndexStorageManager, LONG_INDEX_BINARY_OBJECT_FACTORY.get(), LONG_INDEX_BINARY_OBJECT_FACTORY.get());
        uniqueQueryableIndex.addIndex(1L, 10L);
        uniqueQueryableIndex.addIndex(2L, 20L);
        uniqueQueryableIndex.addIndex(3L, 30L);
        uniqueQueryableIndex.addIndex(4L, 40L);

        // Test Larger than 1
        Iterator<Long> largerThanIterator = uniqueQueryableIndex.getLessThan(4L, Order.ASC);
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(10L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(20L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(30L, largerThanIterator.next());
        Assertions.assertFalse(largerThanIterator.hasNext());

        // Test Larger than 2
        largerThanIterator = uniqueQueryableIndex.getLessThan(3L, Order.ASC);
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(10L, largerThanIterator.next());
        Assertions.assertTrue(largerThanIterator.hasNext());
        Assertions.assertEquals(20L, largerThanIterator.next());
        Assertions.assertFalse(largerThanIterator.hasNext());


        // Test Larger than 10
        largerThanIterator = uniqueQueryableIndex.getLessThan(1L, Order.ASC);
        Assertions.assertFalse(largerThanIterator.hasNext());


        // Test larger than equal 1
        Iterator<Long> largerThanEQIterator = uniqueQueryableIndex.getLessThanEqual(4L, Order.ASC);
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(10L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(20L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(30L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(40L, largerThanEQIterator.next());
        Assertions.assertFalse(largerThanEQIterator.hasNext());

        // Test lte 3
        largerThanEQIterator = uniqueQueryableIndex.getLessThanEqual(2L, Order.ASC);
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(10L, largerThanEQIterator.next());
        Assertions.assertTrue(largerThanEQIterator.hasNext());
        Assertions.assertEquals(20L, largerThanEQIterator.next());
        Assertions.assertFalse(largerThanEQIterator.hasNext());

        // Test lte 0
        largerThanEQIterator = uniqueQueryableIndex.getLessThanEqual(0L, Order.DESC);
        Assertions.assertFalse(largerThanEQIterator.hasNext());
    }

}
