package com.github.sepgh.test.operation;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.*;
import com.github.sepgh.testudo.scheme.ModelToCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.annotation.Collection;
import com.github.sepgh.testudo.scheme.annotation.Field;
import com.github.sepgh.testudo.scheme.annotation.Index;
import com.github.sepgh.testudo.storage.db.DiskPageDatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.DefaultIndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.header.InMemoryIndexHeaderManager;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.github.sepgh.testudo.utils.ReaderWriterLock;
import lombok.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class CollectionSelectInsertOperationMultiThreadedTestCase {

    private EngineConfig engineConfig;
    private Path dbPath;

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
    }

    private DiskPageDatabaseStorageManager getDiskPageDatabaseStorageManager() {
        return new DiskPageDatabaseStorageManager(
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


    @Test
    public void test2() throws InternalOperationException, IndexExistsException {
        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .build();

        DiskPageDatabaseStorageManager storageManager = getDiskPageDatabaseStorageManager();
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new InMemoryIndexHeaderManager.Factory());
        CollectionIndexProviderFactory collectionIndexProviderFactory = new DefaultCollectionIndexProviderFactory(scheme, engineConfig, indexStorageManagerFactory, storageManager);


        Scheme.Collection collection = new ModelToCollectionConverter(TestModel.class).toCollection();
        scheme.getCollections().add(collection);

        CollectionIndexProvider collectionIndexProvider = collectionIndexProviderFactory.create(collection);
        UniqueTreeIndexManager<Long, Pointer> clusterIndexManager = (UniqueTreeIndexManager<Long, Pointer>) collectionIndexProvider.getClusterIndexManager();

        int i = 0;

        while (i < 1000) {
            Long l = clusterIndexManager.nextKey();
            clusterIndexManager.addIndex(l, Pointer.empty());
            i++;
        }


    }

    @Test
    public void test() throws IOException, SerializationException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .build();

        DiskPageDatabaseStorageManager storageManager = getDiskPageDatabaseStorageManager();
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new InMemoryIndexHeaderManager.Factory());
        CollectionIndexProviderFactory collectionIndexProviderFactory = new DefaultCollectionIndexProviderFactory(scheme, engineConfig, indexStorageManagerFactory, storageManager);


        Scheme.Collection collection = new ModelToCollectionConverter(TestModel.class).toCollection();
        scheme.getCollections().add(collection);

        DefaultCollectionInsertOperation<Long> collectionInsertOperation = new DefaultCollectionInsertOperation<>(scheme, collection, new ReaderWriterLock(), collectionIndexProviderFactory, storageManager);

        ExecutorService executorService = Executors.newFixedThreadPool(9);
        CountDownLatch countDownLatch = new CountDownLatch(45);
        final AtomicInteger atomicInteger = new AtomicInteger();

        for (int i = 0; i < 5; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                     for (int j = 1; j < 10; j++) {
                         try {
                             int i1 = atomicInteger.incrementAndGet();
                             collectionInsertOperation.insert(
                                     TestModel.builder()
                                             .id(i1)
                                             .item(j)
                                             .build()
                             );

                             /*UniqueTreeIndexManager<Integer, Pointer> uniqueTreeIndexManager = (UniqueTreeIndexManager<Integer, Pointer>) collectionInsertOperation.getCollectionIndexProvider().getDuplicateIndexManager("item").getInnerIndexManager();
                             Optional<Pointer> index = uniqueTreeIndexManager.getIndex(j);
                             Pointer p = index.get();
                             System.out.println(">> " + p);*/
                         } catch (SerializationException | RuntimeException e) {
                             e.printStackTrace();
                         } finally {
                             countDownLatch.countDown();
//                             System.out.println("countdown: " + countDownLatch.getCount());
                         }
                     }
                }
            });
        }

        countDownLatch.await();

        CollectionSelectOperation<Long> collectionSelectOperation = new DefaultCollectionSelectOperation<>(collection, new ReaderWriterLock(), collectionIndexProviderFactory, storageManager);
        long count = collectionSelectOperation.count();
        Assertions.assertEquals(45L, count);

    }


}
