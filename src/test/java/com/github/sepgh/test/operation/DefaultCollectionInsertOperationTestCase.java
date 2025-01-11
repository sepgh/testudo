package com.github.sepgh.test.operation;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.SerializationException;
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
import java.util.List;
import java.util.concurrent.ExecutionException;

public class DefaultCollectionInsertOperationTestCase {

    private EngineConfig engineConfig;
    private Path dbPath;

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

        @Field(id = 2, maxLength = 20)
        private String name;

        @Field(id = 3)
        @Index()
        private Long age;

        @Field(id = 4, name = "country_code")
        @Index(lowCardinality = true)
        private String country;

    }

    @Data
    @Collection(id = 1, name = "test")
    @Builder
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NullableTestModel {
        @Field(id = 1)
        @Index(primary = true, autoIncrement = true)
        private Integer id;

        @Field(id = 2, maxLength = 20)
        private String name;

        @Field(id = 3, nullable = true)
        @Index()
        private Long age;

        @Field(id = 4, name = "country_code", nullable = true)
        @Index(lowCardinality = true)
        private String country;
    }



    @Test
    public void test() throws IOException, SerializationException, ExecutionException, InterruptedException, InternalOperationException, DeserializationException {
        DatabaseStorageManagerSingletonFactory databaseStorageManagerSingletonFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerSingletonFactory.getInstance();
        IndexStorageManagerSingletonFactory indexStorageManagerSingletonFactory = new DefaultIndexStorageManagerSingletonFactory(this.engineConfig, new JsonIndexHeaderManager.SingletonFactory(), fileHandlerPoolSingletonFactory, databaseStorageManagerSingletonFactory);

        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .build();
        Scheme.Collection collection = new ModelToCollectionConverter(TestModel.class).toCollection();
        scheme.getCollections().add(collection);

        CollectionIndexProviderSingletonFactory collectionIndexProviderSingletonFactory = new DefaultCollectionIndexProviderSingletonFactory(scheme, engineConfig, indexStorageManagerSingletonFactory, storageManager);

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

        execute = collectionSelectOperation.query(
                new Query().where(new SimpleCondition<>("age", Operation.GT, 10L))
        ).execute(TestModel.class);
        Assertions.assertTrue(execute.hasNext());
        Assertions.assertEquals(execute.next(), testModel2);
        Assertions.assertTrue(execute.hasNext());
        Assertions.assertEquals(execute.next(), testModel3);
        Assertions.assertTrue(execute.hasNext());
        Assertions.assertEquals(execute.next(), testModel4);
        Assertions.assertFalse(execute.hasNext());


        long deleted = collectionDeleteOperation.execute();
        Assertions.assertEquals(4, deleted);

    }


    @Test
    public void test_nullable() throws SerializationException, InternalOperationException, DeserializationException {
        DatabaseStorageManagerSingletonFactory databaseStorageManagerSingletonFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerSingletonFactory.getInstance();
        IndexStorageManagerSingletonFactory indexStorageManagerSingletonFactory = new DefaultIndexStorageManagerSingletonFactory(this.engineConfig, new JsonIndexHeaderManager.SingletonFactory(), fileHandlerPoolSingletonFactory, databaseStorageManagerSingletonFactory);

        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .build();
        Scheme.Collection collection = new ModelToCollectionConverter(NullableTestModel.class).toCollection();
        scheme.getCollections().add(collection);

        CollectionIndexProviderSingletonFactory collectionIndexProviderSingletonFactory = new DefaultCollectionIndexProviderSingletonFactory(scheme, engineConfig, indexStorageManagerSingletonFactory, storageManager);

        NullableTestModel m1 = NullableTestModel.builder()
                .id(1)
                .name("John")
                .age(null)
                .country("DE")
                .build();
        NullableTestModel m2 = NullableTestModel.builder()
                .id(2)
                .name("Rose")
                .age(20L)
                .build();


        ReaderWriterLock readerWriterLock = new ReaderWriterLock();
        CollectionInsertOperation<Long> collectionInsertOperation = new DefaultCollectionInsertOperation<>(scheme, collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);
        CollectionSelectOperation<Long> collectionSelectOperation = new DefaultCollectionSelectOperation<>(collection, readerWriterLock, collectionIndexProviderSingletonFactory.getInstance(collection), storageManager);

        for (NullableTestModel testModel : Arrays.asList(m1, m2)) {
            collectionInsertOperation.execute(testModel);
        }

        List<NullableTestModel> nullCountries = collectionSelectOperation.query(new Query("country_code", Operation.IS_NULL)).asList(NullableTestModel.class);
        Assertions.assertEquals(1, nullCountries.size());
        Assertions.assertEquals(m2, nullCountries.getFirst());
        Assertions.assertNull(m2.country);


        List<NullableTestModel> nullAges = collectionSelectOperation.query(new Query("age", Operation.IS_NULL)).asList(NullableTestModel.class);
        Assertions.assertEquals(1, nullAges.size());
        Assertions.assertEquals(m1, nullAges.getFirst());
        Assertions.assertNull(m1.age);
    }

}
