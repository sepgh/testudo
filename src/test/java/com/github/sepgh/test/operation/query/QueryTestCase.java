package com.github.sepgh.test.operation.query;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.DuplicateQueryableIndex;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueQueryableIndex;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.CollectionIndexProvider;
import com.github.sepgh.testudo.operation.CollectionIndexProviderFactory;
import com.github.sepgh.testudo.operation.DefaultCollectionIndexProviderFactory;
import com.github.sepgh.testudo.operation.query.Operation;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.operation.query.SimpleCondition;
import com.github.sepgh.testudo.operation.query.SortField;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.db.DiskPageDatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.DefaultIndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedInteger;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class QueryTestCase {
    private DatabaseStorageManager databaseStorageManager;
    private Path dbPath;
    private EngineConfig engineConfig;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory("TEST_" + this.getClass().getSimpleName());
        this.engineConfig = EngineConfig.builder()
                .baseDBPath(this.dbPath.toString())
                .clusterKeyType(EngineConfig.ClusterKeyType.UINT)
                .bTreeDegree(10)
                .build();
        this.databaseStorageManager = new DiskPageDatabaseStorageManager(
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

    @Test
    @Timeout(value = 2)
    public void simpleCondition() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new JsonIndexHeaderManager.Factory());
        CollectionIndexProviderFactory collectionIndexProviderFactory = new DefaultCollectionIndexProviderFactory(engineConfig, indexStorageManagerFactory, this.databaseStorageManager);

        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .collections(
                        List.of(
                                Scheme.Collection.builder()
                                        .id(1)
                                        .name("test_collection")
                                        .fields(
                                                List.of(
                                                        Scheme.Field.builder()
                                                                .id(1)
                                                                .index(true)
                                                                .indexUnique(true)
                                                                .type("int")
                                                                .name("pk")
                                                                .build(),
                                                        Scheme.Field.builder()
                                                                .id(2)
                                                                .index(true)
                                                                .type("int")
                                                                .name("age")
                                                                .build()
                                                )
                                        )
                                        .build()
                        )
                )
                .build();
        Scheme.Collection collection = scheme.getCollections().getFirst();
        CollectionIndexProvider collectionIndexProvider = collectionIndexProviderFactory.create(collection);


        Query query = new Query();
        query.where(
                new SimpleCondition<>(
                        collection,
                        "pk",
                        Operation.GT,
                        10
                )
        );
        List<UnsignedInteger> executedResults = Lists.newArrayList(query.execute(collectionIndexProvider));;
        Assertions.assertTrue(executedResults.isEmpty());   // NO DATA



        // --- ADD DATA TO THE COLLECTION --- //
        UniqueTreeIndexManager<UnsignedInteger, Pointer> clusterIndexManager = (UniqueTreeIndexManager<UnsignedInteger, Pointer>) collectionIndexProvider.getClusterIndexManager();
        UniqueQueryableIndex<Integer, UnsignedInteger> pkIndexManager = (UniqueQueryableIndex<Integer, UnsignedInteger>) collectionIndexProvider.getUniqueIndexManager(collection.getFields().getFirst());
        DuplicateQueryableIndex<Integer, UnsignedInteger> ageIndexManager = (DuplicateQueryableIndex<Integer, UnsignedInteger>) collectionIndexProvider.getDuplicateIndexManager(collection.getFields().getLast());

        byte[] data = new byte[]{
                0x00, 0x00, 0x00, 0x01,    // PK
                0x00, 0x00, 0x00, 0x01     // AGE  = 1
        };
        Pointer pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(1L), pointer);
        pkIndexManager.addIndex(1, UnsignedInteger.valueOf(1L));
        ageIndexManager.addIndex(1, UnsignedInteger.valueOf(1L));

        data = new byte[]{
                0x00, 0x00, 0x00, 0x02,    // PK
                0x00, 0x00, 0x00, 0x01     // AGE  = 1
        };
        pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(2L), pointer);
        pkIndexManager.addIndex(2, UnsignedInteger.valueOf(2L));
        ageIndexManager.addIndex(1, UnsignedInteger.valueOf(2L));

        data = new byte[]{
                0x00, 0x00, 0x00, 0x03,    // PK
                0x00, 0x00, 0x00, 0x03     // AGE  =  3
        };
        pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(3L), pointer);
        pkIndexManager.addIndex(3, UnsignedInteger.valueOf(3L));
        ageIndexManager.addIndex(3, UnsignedInteger.valueOf(3L));


        //-- TEST QUERIES --//


        query = new Query();
        query.where(
                new SimpleCondition<>(
                        collection,
                        "pk",
                        Operation.GT,
                        1
                )
        );
        executedResults = Lists.newArrayList(query.execute(collectionIndexProvider));;
        Assertions.assertEquals(2, executedResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(2), executedResults.getFirst());
        Assertions.assertEquals(UnsignedInteger.valueOf(3), executedResults.getLast());


        query = new Query();
        query.where(
                new SimpleCondition<>(
                        collection,
                        "pk",
                        Operation.GTE,
                        1
                )
        );
        executedResults = Lists.newArrayList(query.execute(collectionIndexProvider));;
        Assertions.assertEquals(3, executedResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(1), executedResults.getFirst());
        Assertions.assertEquals(UnsignedInteger.valueOf(2), executedResults.get(1));
        Assertions.assertEquals(UnsignedInteger.valueOf(3), executedResults.getLast());


        query = new Query();
        query.where(
                new SimpleCondition<>(
                        collection,
                        "pk",
                        Operation.GTE,
                        1
                )
        );
        executedResults = Lists.newArrayList(query.execute(collectionIndexProvider));;
        Assertions.assertEquals(3, executedResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(1), executedResults.getFirst());
        Assertions.assertEquals(UnsignedInteger.valueOf(2), executedResults.get(1));
        Assertions.assertEquals(UnsignedInteger.valueOf(3), executedResults.getLast());

        query = new Query();
        query.where(
                new SimpleCondition<>(
                        collection,
                        "age",
                        Operation.LTE,
                        1
                )
        );
        executedResults = Lists.newArrayList(query.execute(collectionIndexProvider));;
        Assertions.assertEquals(2, executedResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(1), executedResults.getFirst());
        Assertions.assertEquals(UnsignedInteger.valueOf(2), executedResults.get(1));

        query = new Query();
        query.where(
                new SimpleCondition<>(
                        collection,
                        "age",
                        Operation.EQ,
                        1
                )
        );
        executedResults = Lists.newArrayList(query.execute(collectionIndexProvider));;
        Assertions.assertEquals(2, executedResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(1), executedResults.getFirst());
        Assertions.assertEquals(UnsignedInteger.valueOf(2), executedResults.get(1));

    }

    @Test
    @Timeout(value = 2)
    public void simpleCondition_LowCardinality() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new JsonIndexHeaderManager.Factory());
        CollectionIndexProviderFactory collectionIndexProviderFactory = new DefaultCollectionIndexProviderFactory(engineConfig, indexStorageManagerFactory, this.databaseStorageManager);

        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .collections(
                        List.of(
                                Scheme.Collection.builder()
                                        .id(1)
                                        .name("test_collection")
                                        .fields(
                                                List.of(
                                                        Scheme.Field.builder()
                                                                .id(1)
                                                                .index(true)
                                                                .indexUnique(true)
                                                                .type("int")
                                                                .name("pk")
                                                                .build(),
                                                        Scheme.Field.builder()
                                                                .id(2)
                                                                .lowCardinality(true)
                                                                .index(true)
                                                                .type("int")
                                                                .name("age")
                                                                .build()
                                                )
                                        )
                                        .build()
                        )
                )
                .build();
        Scheme.Collection collection = scheme.getCollections().getFirst();
        CollectionIndexProvider collectionIndexProvider = collectionIndexProviderFactory.create(collection);


        // --- ADD DATA TO THE COLLECTION --- //
        UniqueTreeIndexManager<UnsignedInteger, Pointer> clusterIndexManager = (UniqueTreeIndexManager<UnsignedInteger, Pointer>) collectionIndexProvider.getClusterIndexManager();
        UniqueQueryableIndex<Integer, UnsignedInteger> pkIndexManager = (UniqueQueryableIndex<Integer, UnsignedInteger>) collectionIndexProvider.getUniqueIndexManager(collection.getFields().getFirst());
        DuplicateQueryableIndex<Integer, UnsignedInteger> ageIndexManager = (DuplicateQueryableIndex<Integer, UnsignedInteger>) collectionIndexProvider.getDuplicateIndexManager(collection.getFields().getLast());

        byte[] data = new byte[]{
                0x00, 0x00, 0x00, 0x01,    // PK
                0x00, 0x00, 0x00, 0x01     // AGE  = 1
        };
        Pointer pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(1L), pointer);
        pkIndexManager.addIndex(1, UnsignedInteger.valueOf(1L));
        ageIndexManager.addIndex(1, UnsignedInteger.valueOf(1L));

        data = new byte[]{
                0x00, 0x00, 0x00, 0x02,    // PK
                0x00, 0x00, 0x00, 0x01     // AGE  = 1
        };
        pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(2L), pointer);
        pkIndexManager.addIndex(2, UnsignedInteger.valueOf(2L));
        ageIndexManager.addIndex(1, UnsignedInteger.valueOf(2L));

        data = new byte[]{
                0x00, 0x00, 0x00, 0x03,    // PK
                0x00, 0x00, 0x00, 0x03     // AGE  =  3
        };
        pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(3L), pointer);
        pkIndexManager.addIndex(3, UnsignedInteger.valueOf(3L));
        ageIndexManager.addIndex(3, UnsignedInteger.valueOf(3L));


        Query query = new Query();
        query.where(
                new SimpleCondition<>(
                        collection,
                        "age",
                        Operation.LTE,
                        1
                )
        );
        List<UnsignedInteger> executedResults = Lists.newArrayList(query.execute(collectionIndexProvider));;
        Assertions.assertEquals(2, executedResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(1), executedResults.getFirst());
        Assertions.assertEquals(UnsignedInteger.valueOf(2), executedResults.get(1));
    }

    @SneakyThrows
    @Test
    @Timeout(2)
    public void compositeQuery_And() {
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new JsonIndexHeaderManager.Factory());
        CollectionIndexProviderFactory collectionIndexProviderFactory = new DefaultCollectionIndexProviderFactory(engineConfig, indexStorageManagerFactory, this.databaseStorageManager);

        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .collections(
                        List.of(
                                Scheme.Collection.builder()
                                        .id(1)
                                        .name("test_collection")
                                        .fields(
                                                List.of(
                                                        Scheme.Field.builder()
                                                                .id(1)
                                                                .index(true)
                                                                .indexUnique(true)
                                                                .type("int")
                                                                .name("pk")
                                                                .build(),
                                                        Scheme.Field.builder()
                                                                .id(2)
                                                                .index(true)
                                                                .type("int")
                                                                .name("age")
                                                                .build()
                                                )
                                        )
                                        .build()
                        )
                )
                .build();
        Scheme.Collection collection = scheme.getCollections().getFirst();
        CollectionIndexProvider collectionIndexProvider = collectionIndexProviderFactory.create(collection);


        // --- ADD DATA TO THE COLLECTION --- //
        UniqueTreeIndexManager<UnsignedInteger, Pointer> clusterIndexManager = (UniqueTreeIndexManager<UnsignedInteger, Pointer>) collectionIndexProvider.getClusterIndexManager();
        UniqueQueryableIndex<Integer, UnsignedInteger> pkIndexManager = (UniqueQueryableIndex<Integer, UnsignedInteger>) collectionIndexProvider.getUniqueIndexManager(collection.getFields().getFirst());
        DuplicateQueryableIndex<Integer, UnsignedInteger> ageIndexManager = (DuplicateQueryableIndex<Integer, UnsignedInteger>) collectionIndexProvider.getDuplicateIndexManager(collection.getFields().getLast());

        byte[] data = new byte[]{
                0x00, 0x00, 0x00, 0x01,    // PK
                0x00, 0x00, 0x00, 0x01     // AGE  = 1
        };
        Pointer pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(1L), pointer);
        pkIndexManager.addIndex(1, UnsignedInteger.valueOf(1L));
        ageIndexManager.addIndex(1, UnsignedInteger.valueOf(1L));

        data = new byte[]{
                0x00, 0x00, 0x00, 0x02,    // PK
                0x00, 0x00, 0x00, 0x01     // AGE  = 1
        };
        pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(2L), pointer);
        pkIndexManager.addIndex(2, UnsignedInteger.valueOf(2L));
        ageIndexManager.addIndex(1, UnsignedInteger.valueOf(2L));

        data = new byte[]{
                0x00, 0x00, 0x00, 0x03,    // PK
                0x00, 0x00, 0x00, 0x03     // AGE  =  3
        };
        pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(3L), pointer);
        pkIndexManager.addIndex(3, UnsignedInteger.valueOf(3L));
        ageIndexManager.addIndex(3, UnsignedInteger.valueOf(3L));


        Query query = new Query();
        query.where(
                new SimpleCondition<>(
                        collection,
                        "pk",
                        Operation.GT,
                        1
                )
        );
        query.and(
                new SimpleCondition<>(
                        collection,
                        "age",
                        Operation.GTE,
                        2
                )
        );
        List<UnsignedInteger> queryResults = Lists.newArrayList(query.execute(collectionIndexProvider));;
        Assertions.assertEquals(1, queryResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(3), queryResults.getFirst());

        query = new Query();
        query.where(
                new SimpleCondition<>(
                        collection,
                        "pk",
                        Operation.GT,
                        1
                )
        );
        query.and(
                new SimpleCondition<>(
                        collection,
                        "age",
                        Operation.GTE,
                        1
                )
        );
        query = query.sort(new SortField(collection.getFields().getFirst(), Order.DESC));
        query = query.offset(0);
        queryResults = Lists.newArrayList(query.execute(collectionIndexProvider));
        Assertions.assertEquals(2, queryResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(3), queryResults.getFirst());
        Assertions.assertEquals(UnsignedInteger.valueOf(2), queryResults.getLast());

        queryResults = Lists.newArrayList(query.limit(1).execute(collectionIndexProvider));
        Assertions.assertEquals(1, queryResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(3), queryResults.getFirst());

        queryResults = Lists.newArrayList(query.offset(1).execute(collectionIndexProvider));
        Assertions.assertEquals(1, queryResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(2), queryResults.getFirst());
    }


    @SneakyThrows
    @Test
    @Timeout(2)
    public void compositeQuery_Or() {
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new JsonIndexHeaderManager.Factory());
        CollectionIndexProviderFactory collectionIndexProviderFactory = new DefaultCollectionIndexProviderFactory(engineConfig, indexStorageManagerFactory, this.databaseStorageManager);

        Scheme scheme = Scheme.builder()
                .dbName("test")
                .version(1)
                .collections(
                        List.of(
                                Scheme.Collection.builder()
                                        .id(1)
                                        .name("test_collection")
                                        .fields(
                                                List.of(
                                                        Scheme.Field.builder()
                                                                .id(1)
                                                                .index(true)
                                                                .indexUnique(true)
                                                                .type("int")
                                                                .name("pk")
                                                                .build(),
                                                        Scheme.Field.builder()
                                                                .id(2)
                                                                .index(true)
                                                                .type("int")
                                                                .name("age")
                                                                .build()
                                                )
                                        )
                                        .build()
                        )
                )
                .build();
        Scheme.Collection collection = scheme.getCollections().getFirst();
        CollectionIndexProvider collectionIndexProvider = collectionIndexProviderFactory.create(collection);


        // --- ADD DATA TO THE COLLECTION --- //
        UniqueTreeIndexManager<UnsignedInteger, Pointer> clusterIndexManager = (UniqueTreeIndexManager<UnsignedInteger, Pointer>) collectionIndexProvider.getClusterIndexManager();
        UniqueQueryableIndex<Integer, UnsignedInteger> pkIndexManager = (UniqueQueryableIndex<Integer, UnsignedInteger>) collectionIndexProvider.getUniqueIndexManager(collection.getFields().getFirst());
        DuplicateQueryableIndex<Integer, UnsignedInteger> ageIndexManager = (DuplicateQueryableIndex<Integer, UnsignedInteger>) collectionIndexProvider.getDuplicateIndexManager(collection.getFields().getLast());

        byte[] data = new byte[]{
                0x00, 0x00, 0x00, 0x01,    // PK
                0x00, 0x00, 0x00, 0x01     // AGE  = 1
        };
        Pointer pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(1L), pointer);
        pkIndexManager.addIndex(1, UnsignedInteger.valueOf(1L));
        ageIndexManager.addIndex(1, UnsignedInteger.valueOf(1L));

        data = new byte[]{
                0x00, 0x00, 0x00, 0x02,    // PK
                0x00, 0x00, 0x00, 0x01     // AGE  = 1
        };
        pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(2L), pointer);
        pkIndexManager.addIndex(2, UnsignedInteger.valueOf(2L));
        ageIndexManager.addIndex(1, UnsignedInteger.valueOf(2L));

        data = new byte[]{
                0x00, 0x00, 0x00, 0x03,    // PK
                0x00, 0x00, 0x00, 0x03     // AGE  =  3
        };
        pointer = this.databaseStorageManager.store(1, 1, data);
        clusterIndexManager.addIndex(UnsignedInteger.valueOf(3L), pointer);
        pkIndexManager.addIndex(3, UnsignedInteger.valueOf(3L));
        ageIndexManager.addIndex(3, UnsignedInteger.valueOf(3L));


        Query query = new Query();
        query.where(
                new SimpleCondition<>(
                        collection,
                        "pk",
                        Operation.GT,
                        3
                )
        );
        query.or(
                new SimpleCondition<>(
                        collection,
                        "age",
                        Operation.GTE,
                        1
                )
        );
        List<UnsignedInteger> queryResults = Lists.newArrayList(query.execute(collectionIndexProvider));;
        Assertions.assertEquals(3, queryResults.size());
        Assertions.assertEquals(UnsignedInteger.valueOf(1), queryResults.getFirst());
        Assertions.assertEquals(UnsignedInteger.valueOf(2), queryResults.get(1));
        Assertions.assertEquals(UnsignedInteger.valueOf(3), queryResults.getLast());

    }
}
