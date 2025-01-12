package com.github.sepgh.test.operation;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.BaseSerializationException;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.operation.*;
import com.github.sepgh.testudo.operation.query.Operation;
import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.operation.query.SimpleCondition;
import com.github.sepgh.testudo.scheme.ModelToCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.annotation.Collection;
import com.github.sepgh.testudo.scheme.annotation.Field;
import com.github.sepgh.testudo.scheme.annotation.Index;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManagerSingletonFactory;
import com.github.sepgh.testudo.storage.index.DefaultIndexStorageManagerSingletonFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerSingletonFactory;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandlerPoolSingletonFactory;
import com.github.sepgh.testudo.utils.ReaderWriterLock;
import lombok.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

public class DefaultCollectionUpdateOperationTestCase {

    private EngineConfig engineConfig;
    private Path dbPath;
    private final Scheme scheme = Scheme.builder()
            .dbName("test")
            .version(1)
            .build();
    private FileHandlerPoolSingletonFactory fileHandlerPoolSingletonFactory;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory(this.getClass().getSimpleName());
        this.engineConfig = EngineConfig.builder()
                .clusterKeyType(EngineConfig.ClusterKeyType.LONG)
                .baseDBPath(this.dbPath.toString())
                .build();
        this.fileHandlerPoolSingletonFactory = new FileHandlerPoolSingletonFactory.DefaultFileHandlerPoolSingletonFactory(engineConfig);
    }

    private DatabaseStorageManagerSingletonFactory getDatabaseStorageManagerFactory() {
        return new DatabaseStorageManagerSingletonFactory.DiskPageDatabaseStorageManagerSingletonFactory(engineConfig, fileHandlerPoolSingletonFactory);
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
        @Index(primary = true, autoIncrement = true)
        private Integer id;

        @Field(id = 2, maxLength = 20, nullable = true)
        private String name;

        @Field(id = 3)
        @Index()
        private Long age;

        @Field(id = 4, name = "country_code", nullable = true)
        @Index(lowCardinality = true)
        private String country;

    }

    @Test
    public void test() throws IOException, BaseSerializationException, ExecutionException, InterruptedException, InternalOperationException {
        DatabaseStorageManagerSingletonFactory databaseStorageManagerSingletonFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerSingletonFactory.getInstance();
        IndexStorageManagerSingletonFactory indexStorageManagerSingletonFactory = new DefaultIndexStorageManagerSingletonFactory(this.engineConfig, new JsonIndexHeaderManager.SingletonFactory(), fileHandlerPoolSingletonFactory, databaseStorageManagerSingletonFactory);
        CollectionIndexProviderSingletonFactory collectionIndexProviderSingletonFactory = new DefaultCollectionIndexProviderSingletonFactory(scheme, engineConfig, indexStorageManagerSingletonFactory, storageManager);

        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .build();
        Scheme.Collection collection = new ModelToCollectionConverter(TestModel.class).toCollection();
        scheme.getCollections().add(collection);

        TestModel testModel1 = TestModel.builder().id(1).age(10L).country("DE").name("John").build();
        TestModel testModel2 = TestModel.builder().id(2).age(20L).country("FR").name("Rose").build();
        TestModel testModel3 = TestModel.builder().id(3).age(30L).country("USA").name("Jack").build();
        TestModel testModel4 = TestModel.builder().id(4).age(40L).country("GB").name("Foo").build();

        ReaderWriterLock readerWriterLock = new ReaderWriterLock();
        CollectionInsertOperation<Long> collectionInsertOperation = new DefaultCollectionInsertOperation<>(scheme, collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);
        CollectionDeleteOperation<Long> collectionDeleteOperation = new DefaultCollectionDeleteOperation<>(collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);

        for (TestModel testModel : Arrays.asList(testModel1, testModel2, testModel3, testModel4)) {
            collectionInsertOperation.execute(testModel);
        }

        CollectionSelectOperation<Long> collectionSelectOperation = new DefaultCollectionSelectOperation<>(collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);
        long count = collectionSelectOperation.count();
        Assertions.assertEquals(4L, count);
        Iterator<TestModel> execute = collectionSelectOperation.execute(TestModel.class);
        Assertions.assertTrue(execute.hasNext());
        Assertions.assertEquals(execute.next(), testModel1);
        Assertions.assertTrue(execute.hasNext());
        Assertions.assertEquals(execute.next(), testModel2);
        Assertions.assertTrue(execute.hasNext());
        Assertions.assertEquals(execute.next(), testModel3);
        Assertions.assertTrue(execute.hasNext());
        Assertions.assertEquals(execute.next(), testModel4);
        Assertions.assertFalse(execute.hasNext());


        Query query = new Query().where(new SimpleCondition<>("age", Operation.GT, 10L));
        execute = collectionSelectOperation.query(query).execute(TestModel.class);
        Assertions.assertTrue(execute.hasNext());
        Assertions.assertEquals(execute.next(), testModel2);
        Assertions.assertTrue(execute.hasNext());
        Assertions.assertEquals(execute.next(), testModel3);
        Assertions.assertTrue(execute.hasNext());
        Assertions.assertEquals(execute.next(), testModel4);
        Assertions.assertFalse(execute.hasNext());


        CollectionUpdateOperation<Long> collectionUpdateOperation = new DefaultCollectionUpdateOperation<>(collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);
        collectionUpdateOperation.query(query);
        long updates = collectionUpdateOperation.execute(TestModel.class, (testModel) -> {
            testModel.setAge(testModel.getAge() + 100);
        });

        Assertions.assertEquals(3, updates);

        query = new Query().where(new SimpleCondition<>("age", Operation.GT, 100L));
        long ageCount = collectionSelectOperation.query(query).count();
        Assertions.assertEquals(ageCount, 3L);

        collectionUpdateOperation.query(new Query()).execute(TestModel.class, (testModel) -> {
            testModel.setCountry("FR");
        });

        query = new Query().where(new SimpleCondition<>("country_code", Operation.EQ, "FR"));
        long countryCount = collectionSelectOperation.query(query).count();
        Assertions.assertEquals(countryCount, 4L);

        long deleted = collectionDeleteOperation.execute();
        Assertions.assertEquals(4, deleted);
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Collection(id=1)
    public static class UniqueModel {
        @Field(id=1)
        @Index(unique = true)
        private Integer uq;
    }

    @Test
    public void testUniqueUpdate() throws BaseSerializationException, InternalOperationException {
        DatabaseStorageManagerSingletonFactory databaseStorageManagerSingletonFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerSingletonFactory.getInstance();
        IndexStorageManagerSingletonFactory indexStorageManagerSingletonFactory = new DefaultIndexStorageManagerSingletonFactory(this.engineConfig, new JsonIndexHeaderManager.SingletonFactory(), fileHandlerPoolSingletonFactory, databaseStorageManagerSingletonFactory);
        CollectionIndexProviderSingletonFactory collectionIndexProviderSingletonFactory = new DefaultCollectionIndexProviderSingletonFactory(scheme, engineConfig, indexStorageManagerSingletonFactory, storageManager);

        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .build();
        Scheme.Collection collection = new ModelToCollectionConverter(UniqueModel.class).toCollection();
        scheme.getCollections().add(collection);

        UniqueModel model1 = new UniqueModel(1);
        UniqueModel model2 = new UniqueModel(2);
        ReaderWriterLock readerWriterLock = new ReaderWriterLock();

        CollectionInsertOperation<Long> collectionInsertOperation = new DefaultCollectionInsertOperation<>(scheme, collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);
        CollectionUpdateOperation<Long> collectionUpdateOperation = new DefaultCollectionUpdateOperation<>(collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);

        collectionInsertOperation.execute(model1);
        collectionInsertOperation.execute(model2);

        Assertions.assertThrowsExactly(IndexExistsException.class, () -> {
            collectionUpdateOperation.query(new Query("uq", Operation.EQ, 2)).execute(UniqueModel.class, uniqueModel -> {
                uniqueModel.setUq(1);
            });
        });
    }

    @Test
    public void testWithNull() throws BaseSerializationException, InternalOperationException {
        DatabaseStorageManagerSingletonFactory databaseStorageManagerSingletonFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerSingletonFactory.getInstance();
        IndexStorageManagerSingletonFactory indexStorageManagerSingletonFactory = new DefaultIndexStorageManagerSingletonFactory(this.engineConfig, new JsonIndexHeaderManager.SingletonFactory(), fileHandlerPoolSingletonFactory, databaseStorageManagerSingletonFactory);
        CollectionIndexProviderSingletonFactory collectionIndexProviderSingletonFactory = new DefaultCollectionIndexProviderSingletonFactory(scheme, engineConfig, indexStorageManagerSingletonFactory, storageManager);

        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .build();
        Scheme.Collection collection = new ModelToCollectionConverter(TestModel.class).toCollection();
        scheme.getCollections().add(collection);

        TestModel testModel1 = TestModel.builder().id(1).age(10L).name("John").build();
        TestModel testModel2 = TestModel.builder().id(2).age(20L).name("Rose").build();
        ReaderWriterLock readerWriterLock = new ReaderWriterLock();

        CollectionInsertOperation<Long> collectionInsertOperation = new DefaultCollectionInsertOperation<>(scheme, collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);
        CollectionSelectOperation<Long> collectionSelectOperation = new DefaultCollectionSelectOperation<>(collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);
        CollectionUpdateOperation<Long> collectionUpdateOperation = new DefaultCollectionUpdateOperation<>(collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);


        for (TestModel testModel : Arrays.asList(testModel1, testModel2)) {
            collectionInsertOperation.execute(testModel);
        }

        long count = collectionSelectOperation.query(new Query().where("country_code", Operation.IS_NULL)).count();
        Assertions.assertEquals(2L, count);

        long updated = collectionUpdateOperation.query(new Query().where("country_code", Operation.IS_NULL)).execute(TestModel.class, (testModel) -> {
            testModel.setCountry("FR");
        });
        Assertions.assertEquals(2L, updated);

        count = collectionSelectOperation.query(new Query().where("country_code", Operation.EQ, "FR")).count();
        Assertions.assertEquals(2L, count);

        count = collectionSelectOperation.query(new Query().where("country_code", Operation.IS_NULL)).count();
        Assertions.assertEquals(0L, count);

        // Nothing really gets updated
        updated = collectionUpdateOperation.query(new Query().where("country_code", Operation.EQ, "FR")).execute(TestModel.class, (testModel) -> {
            testModel.setCountry("FR");
        });
        Assertions.assertEquals(0L, updated);

        updated = collectionUpdateOperation.query(new Query().where("country_code", Operation.EQ, "FR")).execute(TestModel.class, (testModel) -> {
            testModel.setCountry(null);
        });
        Assertions.assertEquals(2L, updated);
    }


}
