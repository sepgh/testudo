package com.github.sepgh.test.operation;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.operation.*;
import com.github.sepgh.testudo.operation.query.*;
import com.github.sepgh.testudo.scheme.ModelToCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.annotation.Collection;
import com.github.sepgh.testudo.scheme.annotation.Field;
import com.github.sepgh.testudo.scheme.annotation.Index;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.DefaultIndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandlerPoolFactory;
import com.github.sepgh.testudo.utils.ReaderWriterLock;
import lombok.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class CollectionMultiThreadedOperationTestCase {

    private EngineConfig engineConfig;
    private Path dbPath;
    private FileHandlerPoolFactory fileHandlerPoolFactory;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory(this.getClass().getSimpleName());
        this.engineConfig = EngineConfig.builder()
//                .indexCache(true)
//                .bTreeDegree(100)
                // Todo: comment and see the problem: changing degree affects the caused problem
                //       possible root cause: read the long-ass comment at CompactFileIndexStorageManager above `getAllocatedSpaceForNewNode`
                //       update: changing default strategy to "ORGANIZED".
                .indexStorageManagerStrategy(EngineConfig.IndexStorageManagerStrategy.PAGE_BUFFER)

                .clusterKeyType(EngineConfig.ClusterKeyType.LONG)
                .indexIOSessionStrategy(EngineConfig.IndexIOSessionStrategy.IMMEDIATE)
                .baseDBPath(this.dbPath.toString())
                .build();

        this.fileHandlerPoolFactory = new FileHandlerPoolFactory.DefaultFileHandlerPoolFactory(engineConfig);
    }

    private DatabaseStorageManagerFactory getDatabaseStorageManagerFactory() {
        return new DatabaseStorageManagerFactory.DiskPageDatabaseStorageManagerFactory(engineConfig, fileHandlerPoolFactory);
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }


    @Data
    @Collection(id = 1, name = "test")
    @Builder
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestModel {
        @Field(id = 1)
        @Index(unique = true)
        private Integer id;
        @Field(id = 2)
        @Index()
        private Integer item;
    }


    // Todo: sometimes delete operation fails :/ multithreading related issue
    //       never fails with degree=100
    @Test
    public void test() throws IOException, SerializationException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .build();

        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerFactory.create();
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new JsonIndexHeaderManager.Factory(), fileHandlerPoolFactory, databaseStorageManagerFactory);
        CollectionIndexProviderFactory collectionIndexProviderFactory = new DefaultCollectionIndexProviderFactory(scheme, engineConfig, indexStorageManagerFactory, storageManager);


        Scheme.Collection collection = new ModelToCollectionConverter(TestModel.class).toCollection();
        scheme.getCollections().add(collection);

        final ReaderWriterLock readerWriterLock = new ReaderWriterLock();
        DefaultCollectionInsertOperation<Long> collectionInsertOperation = new DefaultCollectionInsertOperation<>(scheme, collection, readerWriterLock, collectionIndexProviderFactory, storageManager);

        ExecutorService executorService = Executors.newFixedThreadPool(9);
        CountDownLatch countDownLatch = new CountDownLatch(45);
        final AtomicInteger atomicInteger = new AtomicInteger();

        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                 for (int j = 1; j < 10; j++) {
                     try {
                         int i1 = atomicInteger.incrementAndGet();
                         collectionInsertOperation.insert(
                                 TestModel.builder()
                                         .id(i1)
                                         .item(j)
                                         .build()
                         );

                         CollectionSelectOperation<Long> collectionSelectOperation = new DefaultCollectionSelectOperation<>(collection, readerWriterLock, collectionIndexProviderFactory, storageManager);
                         Assertions.assertTrue(
                                 collectionSelectOperation.query(new Query().where(new SimpleCondition<>("id", Operation.EQ, i1))).exists()
                         );

                     } catch (SerializationException | RuntimeException e) {
                         e.printStackTrace();
                     } finally {
                         countDownLatch.countDown();
                     }
                 }
            });
        }
        countDownLatch.await();

        CollectionSelectOperation<Long> collectionSelectOperation = new DefaultCollectionSelectOperation<>(collection, readerWriterLock, collectionIndexProviderFactory, storageManager);
        long count = collectionSelectOperation.count();
        Assertions.assertEquals(45L, count);

        collectionSelectOperation = new DefaultCollectionSelectOperation<>(collection, readerWriterLock, collectionIndexProviderFactory, storageManager);
        List<TestModel> list = collectionSelectOperation.query(new Query().where(new SimpleCondition<>("id", Operation.EQ, 45))).asList(TestModel.class);
        Assertions.assertFalse(list.isEmpty());
        Assertions.assertTrue(collectionSelectOperation.exists());
        Assertions.assertEquals(1, list.size());
        Assertions.assertEquals(45, list.getFirst().getId());

        collectionSelectOperation = new DefaultCollectionSelectOperation<>(collection, readerWriterLock, collectionIndexProviderFactory, storageManager);
        list = collectionSelectOperation.query(new Query().where(new SimpleCondition<>("id", Operation.GT, 40)).sort(new SortField("id", Order.ASC))).asList(TestModel.class);
        Assertions.assertFalse(list.isEmpty());
        Assertions.assertEquals(5, list.size());
        Assertions.assertEquals(41, list.getFirst().getId());

        CountDownLatch countDownLatch2 = new CountDownLatch(45);
        AtomicInteger atomicInteger2 = new AtomicInteger(45);
        executorService.shutdownNow();

        executorService = Executors.newFixedThreadPool(5);

        for (int i = 0; i < 45; i++) {
            executorService.submit(() -> {
                try {
                    int i1 = atomicInteger2.getAndDecrement();
                    CollectionDeleteOperation<Long> collectionDeleteOperation = new DefaultCollectionDeleteOperation<>(collection, readerWriterLock, collectionIndexProviderFactory, storageManager);
                    CollectionSelectOperation<Long> selectOperation = new DefaultCollectionSelectOperation<>(collection, readerWriterLock, collectionIndexProviderFactory, storageManager);

                    int deleted = collectionDeleteOperation
                            .query(new Query().where(new SimpleCondition<>("id", Operation.EQ, i1)))
                            .execute();

                    Assertions.assertEquals(1, deleted);
                } catch (RuntimeException e) {
                    e.printStackTrace();
                } finally {
                    countDownLatch2.countDown();
                }
            });
        }

        countDownLatch2.await();

        collectionSelectOperation = new DefaultCollectionSelectOperation<>(collection, readerWriterLock, collectionIndexProviderFactory, storageManager);
        Assertions.assertEquals(0, collectionSelectOperation.count());

        executorService.shutdownNow();

    }


}
