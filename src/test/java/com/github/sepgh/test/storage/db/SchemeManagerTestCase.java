package com.github.sepgh.test.storage.db;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.CollectionIndexProviderFactory;
import com.github.sepgh.testudo.operation.DefaultCollectionIndexProviderFactory;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.SchemeManager;
import com.github.sepgh.testudo.serialization.CollectionSerializationUtil;
import com.github.sepgh.testudo.serialization.FieldType;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.DefaultIndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandlerPoolFactory;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedLong;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class SchemeManagerTestCase {

    private Path dbPath;
    private EngineConfig engineConfig;
    private final Scheme scheme = Scheme.builder()
            .dbName("test")
            .version(1)
            .build();
    private FileHandlerPoolFactory fileHandlerPoolFactory;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory(this.getClass().getSimpleName());
        this.engineConfig = EngineConfig.builder()
                .clusterKeyType(EngineConfig.ClusterKeyType.LONG)
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

    @Test
    public void test_SchemeManager() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager databaseStorageManager = databaseStorageManagerFactory.getInstance();

        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new JsonIndexHeaderManager.Factory(), fileHandlerPoolFactory, databaseStorageManagerFactory);
        CollectionIndexProviderFactory collectionIndexProviderFactory = new DefaultCollectionIndexProviderFactory(scheme, engineConfig, indexStorageManagerFactory, databaseStorageManager);
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
                                                                .index(Scheme.Index.builder().primary(true).build())
                                                                .type("int")
                                                                .name("pk")
                                                                .build(),
                                                        Scheme.Field.builder()
                                                                .id(2)
                                                                .type("int")
                                                                .name("age")
                                                                .meta(
                                                                        Scheme.Meta.builder()
                                                                                .build()
                                                                )
                                                                .build()
                                                )
                                        )
                                        .build()
                        )
                )
                .build();

        SchemeManager schemeManager = new SchemeManager(
                engineConfig,
                scheme,
                collectionIndexProviderFactory,
                databaseStorageManager
        );
        schemeManager.update();

        Path path = Path.of(engineConfig.getBaseDBPath(), "scheme.json");
        Assertions.assertTrue(Files.exists(path));

        Gson gson = new GsonBuilder().serializeNulls().create();
        FileReader fileReader = new FileReader(path.toString());
        JsonReader jsonReader = new JsonReader(fileReader);
        Scheme newScheme = gson.fromJson(jsonReader, Scheme.class);

        Assertions.assertEquals(scheme, newScheme);

        schemeManager.update();
        Assertions.assertEquals(scheme, newScheme);
        scheme.setVersion(2);
        schemeManager.update();
        Assertions.assertNotEquals(scheme, newScheme);

        fileReader = new FileReader(path.toString());
        jsonReader = new JsonReader(fileReader);
        Scheme newScheme2 = gson.fromJson(jsonReader, Scheme.class);

        Assertions.assertEquals(scheme, newScheme2);

        UniqueTreeIndexManager<?, ?> uniqueTreeIndexManager = collectionIndexProviderFactory.create(scheme.getCollections().getFirst()).getUniqueIndexManager(
                scheme.getCollections().getFirst().getFields().getFirst()
        );

        Method method = DefaultCollectionIndexProviderFactory.class.getDeclaredMethod("getIndexId", Scheme.Collection.class, Scheme.Field.class);
        method.setAccessible(true);
        Object invoked = method.invoke(collectionIndexProviderFactory, scheme.getCollections().getFirst(), scheme.getCollections().getFirst().getFields().getFirst());

        Assertions.assertEquals(
                invoked.hashCode(),
                uniqueTreeIndexManager.getIndexId()
        );
    }

    /*
    *   This is a complex AF test case so here is the story :D
    *
    *   We begin by creating a scheme with 2 integer fields, then add an object and index it as well.
    *   Then we select that same object from DB and make sure what is stored is valid.
    *
    *   After that we update scheme and add a new field to the same collection.
    *   We expect the SchemeManager to update the collection!
    *   This means that the DBObject at previous location should be marked as deleted.
    *   We check that by A) checking isAlive() method on previous DBObject at old pointer and B) making sure index is updated
    *
    *   Afterward, the new object should be 4 bytes longer and store the data set as `defaultValue` for the new field during scheme update.
    */

    // Todo: test fails now that we are working on cluster index ;)
    @Test
    public void test_SchemeManager_WithData() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager databaseStorageManager = databaseStorageManagerFactory.getInstance();
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new JsonIndexHeaderManager.Factory(), fileHandlerPoolFactory, databaseStorageManagerFactory);
        CollectionIndexProviderFactory collectionIndexProviderFactory = new DefaultCollectionIndexProviderFactory(scheme, engineConfig, indexStorageManagerFactory, databaseStorageManager);

        // --- CREATING BASE SCHEME --- //
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
                                                                .index(Scheme.Index.builder().primary(true).build())
                                                                .type("int")
                                                                .name("pk")
                                                                .build(),
                                                        Scheme.Field.builder()
                                                                .id(2)
                                                                .type("int")
                                                                .name("age")
                                                                .meta(
                                                                        Scheme.Meta.builder()
                                                                                .build()
                                                                )
                                                                .build()
                                                )
                                        )
                                        .build()
                        )
                )
                .build();

        // --- UPDATING MANAGER WITH BASE SCHEME --- //
        SchemeManager schemeManager = new SchemeManager(
                engineConfig,
                scheme,
                collectionIndexProviderFactory,
                databaseStorageManager
        );
        schemeManager.update();

        Path path = Path.of(engineConfig.getBaseDBPath(), "scheme.json");
        Assertions.assertTrue(Files.exists(path));

        // --- ADD DATA TO THE COLLECTION --- //
        byte[] data = new byte[]{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01};  // We have 2 integer fields defined in scheme above, so our data is also 2 int fields
        Pointer pointer = databaseStorageManager.store(scheme.getId(), 1, schemeManager.getScheme().getVersion(), data);

        // --- READING DATA AND VERIFYING DB CAN RETURN PROPER OBJ --- //
        DBObject dbObject = databaseStorageManager.select(pointer).get();
        byte[] valueOfField = CollectionSerializationUtil.getValueOfField(
                scheme.getCollections().getFirst(),
                scheme.getCollections().getFirst().getFields().getFirst(),
                dbObject
        );
        int i = Ints.fromByteArray(valueOfField);
        Assertions.assertEquals(1, i);
        Assertions.assertEquals(1, dbObject.getVersion());

        // --- ADDING THE DATA TO INDEX, WHICH IS CLUSTER INDEX AND IS AVAILABLE TO MANAGER TOO --- //
        UniqueTreeIndexManager<UnsignedLong, Pointer> uniqueTreeIndexManager = (UniqueTreeIndexManager<UnsignedLong, Pointer>) collectionIndexProviderFactory.create(scheme.getCollections().getFirst()).getClusterIndexManager();
        Optional<Pointer> optionalPointer = uniqueTreeIndexManager.getIndex(UnsignedLong.valueOf(1));
        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(pointer, optionalPointer.get());


        // --- UPDATING SCHEME, ADD NEW FIELD TO THE END OF COLLECTION --- //
        scheme.setVersion(2);
        ArrayList<Scheme.Field> fields = new ArrayList<>(scheme.getCollections().getFirst().getFields());
        Scheme.Field newField = Scheme.Field.builder()
                .id(100)
                .type(FieldType.INT.getName())
                .name("balance")
                .defaultValue("15")
                .build();
        fields.add(newField);
        scheme.getCollections().getFirst().setFields(fields);

        schemeManager = new SchemeManager(
                engineConfig,
                scheme,
                collectionIndexProviderFactory,
                databaseStorageManager
        );
        schemeManager.update();

        // --- SELECTING OBJECT USING OLD POINTER TO MAKE SURE ITS CHANGED AND IS NO LONGER ALIVE --- //
        dbObject = databaseStorageManager.select(pointer).get();
        Assertions.assertFalse(dbObject.isAlive());
        optionalPointer = uniqueTreeIndexManager.getIndex(UnsignedLong.valueOf(1));
        Assertions.assertNotEquals(pointer, optionalPointer.get());

        // --- SELECTING UPDATED OBJECT AND CHECKING IF DEFAULT VALUE OF NEWLY ADDED FIELD IS SET --- //
        pointer = optionalPointer.get();
        dbObject = databaseStorageManager.select(pointer).get();

        valueOfField = CollectionSerializationUtil.getValueOfField(
                scheme.getCollections().getFirst(),
                newField,
                dbObject
        );
        i = Ints.fromByteArray(valueOfField);
        Assertions.assertEquals(15, i);


        // --- REMOVING THE FIELD SHOULD RESULT IN FIELD GETTING EMPTY IN DB OBJECT --- //
        fields.remove(newField);
        scheme.setVersion(3);
        schemeManager = new SchemeManager(
                engineConfig,
                scheme,
                collectionIndexProviderFactory,
                databaseStorageManager
        );
        schemeManager.update();

        optionalPointer = uniqueTreeIndexManager.getIndex(UnsignedLong.valueOf(1));
        Assertions.assertEquals(pointer, optionalPointer.get());
        dbObject = databaseStorageManager.select(pointer).get();
        Assertions.assertTrue(dbObject.isAlive());

        byte[] bytes = dbObject.readData(2 * Integer.BYTES, Integer.BYTES);
        Assertions.assertEquals(0, Ints.fromByteArray(bytes));


        // --- ADDING NEW FIELD WITH INDEX ON --- //
        scheme.setVersion(4);
        fields = new ArrayList<>(scheme.getCollections().getFirst().getFields());
        int defaultValue = 60;
        newField = Scheme.Field.builder()
                .id(1000)
                .type(FieldType.INT.getName())
                .name("x")
                .defaultValue(String.valueOf(defaultValue))
                .index(Scheme.Index.builder().unique(true).build())
                .build();
        fields.add(newField);
        scheme.getCollections().getFirst().setFields(fields);

        schemeManager = new SchemeManager(
                engineConfig,
                scheme,
                collectionIndexProviderFactory,
                databaseStorageManager
        );
        schemeManager.update();


        UniqueTreeIndexManager<Integer, UnsignedLong> newFieldUniqueTreeIndexManager = (UniqueTreeIndexManager<Integer, UnsignedLong>) collectionIndexProviderFactory.create(scheme.getCollections().getFirst()).getUniqueIndexManager(newField);
        Assertions.assertEquals(1, newFieldUniqueTreeIndexManager.size());
        Optional<UnsignedLong> optionalIndexValue = newFieldUniqueTreeIndexManager.getIndex(defaultValue);
        Assertions.assertTrue(optionalIndexValue.isPresent());
        Assertions.assertEquals(UnsignedLong.valueOf(1L), optionalIndexValue.get());

    }
}
