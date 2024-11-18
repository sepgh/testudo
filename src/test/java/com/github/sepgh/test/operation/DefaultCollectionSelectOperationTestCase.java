package com.github.sepgh.test.operation;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.DuplicateQueryableIndex;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.*;
import com.github.sepgh.testudo.operation.query.Operation;
import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.operation.query.SimpleCondition;
import com.github.sepgh.testudo.scheme.ModelToCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.annotation.AutoIncrement;
import com.github.sepgh.testudo.scheme.annotation.Collection;
import com.github.sepgh.testudo.scheme.annotation.Field;
import com.github.sepgh.testudo.scheme.annotation.Index;
import com.github.sepgh.testudo.serialization.ModelSerializer;
import com.github.sepgh.testudo.storage.db.DiskPageDatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.DefaultIndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
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

public class DefaultCollectionSelectOperationTestCase {

    private EngineConfig engineConfig;
    private Path dbPath;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory(this.getClass().getSimpleName());
        this.engineConfig = EngineConfig.builder()
                .clusterKeyType(EngineConfig.ClusterKeyType.LONG)
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
        @AutoIncrement
        @Index(primary = true)
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


    @Test
    public void test() throws IOException, SerializationException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        DiskPageDatabaseStorageManager storageManager = getDiskPageDatabaseStorageManager();
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new JsonIndexHeaderManager.Factory());
        CollectionIndexProviderFactory collectionIndexProviderFactory = new DefaultCollectionIndexProviderFactory(engineConfig, indexStorageManagerFactory, storageManager);

        Scheme.Collection collection = new ModelToCollectionConverter(TestModel.class).toCollection();
        CollectionIndexProvider collectionIndexProvider = collectionIndexProviderFactory.create(collection);

        TestModel testModel1 = TestModel.builder().id(1).age(10L).country("DE").name("John").build();
        TestModel testModel2 = TestModel.builder().id(2).age(20L).country("FR").name("Rose").build();
        TestModel testModel3 = TestModel.builder().id(3).age(30L).country("USA").name("Jack").build();
        TestModel testModel4 = TestModel.builder().id(4).age(40L).country("GB").name("Foo").build();

        long i = 1;
        for (TestModel testModel : Arrays.asList(testModel1, testModel2, testModel3, testModel4)) {
            ModelSerializer modelSerializer = new ModelSerializer(testModel);
            byte[] serialize = modelSerializer.serialize();

            Pointer pointer = storageManager.store(collection.getId(), 1, serialize);

            UniqueTreeIndexManager<Long, Pointer> clusterIndexManager = (UniqueTreeIndexManager<Long, Pointer>) collectionIndexProvider.getClusterIndexManager();
            clusterIndexManager.addIndex(i, pointer);

            UniqueQueryableIndex<Integer, Long> idIndexManager = (UniqueQueryableIndex<Integer, Long>) collectionIndexProvider.getUniqueIndexManager("id");
            idIndexManager.addIndex(testModel.getId(), i);

            DuplicateQueryableIndex<Long, Long> ageIndexManager = (DuplicateQueryableIndex<Long, Long>) collectionIndexProvider.getDuplicateIndexManager("age");
            ageIndexManager.addIndex(testModel.getAge(), i);

            DuplicateQueryableIndex<String, Long> countryIndexManager = (DuplicateQueryableIndex<String, Long>) collectionIndexProvider.getDuplicateIndexManager("country_code");
            countryIndexManager.addIndex(testModel.getCountry(), i);


            i++;
        }


        CollectionSelectOperation collectionSelectOperation = new DefaultCollectionSelectOperation(collectionIndexProvider, storageManager);
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

        // Todo: uncomment and see err
//        collectionSelectOperation.query(new Query().where(new SimpleCondition<>("age", Operation.GT, 10L))).execute(TestModel.class);
//        Assertions.assertTrue(execute.hasNext());
//        Assertions.assertEquals(execute.next(), testModel2);

    }


}