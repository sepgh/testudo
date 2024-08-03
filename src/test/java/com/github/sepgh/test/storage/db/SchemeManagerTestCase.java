package com.github.sepgh.test.storage.db;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.operation.DefaultFieldIndexManagerProvider;
import com.github.sepgh.testudo.operation.FieldIndexManagerProvider;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.SchemeManager;
import com.github.sepgh.testudo.serialization.CollectionSerializationUtil;
import com.github.sepgh.testudo.serialization.FieldType;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.db.DiskPageDatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.DefaultIndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import com.google.common.hash.HashCode;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class SchemeManagerTestCase {

    private DatabaseStorageManager databaseStorageManager;
    private Path dbPath;
    private EngineConfig engineConfig;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory("TEST_DatabaseStorageManagerTestCase");
        this.engineConfig = EngineConfig.builder()
                .clusterIndexKeyStrategy(EngineConfig.ClusterIndexKeyStrategy.INTEGER)
                .baseDBPath(this.dbPath.toString())
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
    public void test_SchemeManager() throws IOException {
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new JsonIndexHeaderManager.Factory());
        FieldIndexManagerProvider fieldIndexManagerProvider = new DefaultFieldIndexManagerProvider(engineConfig, indexStorageManagerFactory);
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
                                                                .primary(true)
                                                                .type("int")
                                                                .name("pk")
                                                                .build(),
                                                        Scheme.Field.builder()
                                                                .id(2)
                                                                .primary(false)
                                                                .type("int")
                                                                .name("age")
                                                                .meta(
                                                                        Scheme.Meta.builder()
                                                                                .min(15)
                                                                                .max(60)
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
                SchemeManager.SchemeUpdateConfig.builder().build(),
                fieldIndexManagerProvider,
                this.databaseStorageManager
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

        IndexManager<?, ?> indexManager = fieldIndexManagerProvider.getIndexManager(
                scheme.getCollections().getFirst(),
                scheme.getCollections().getFirst().getFields().getFirst()
        );

        Assertions.assertEquals(
                scheme.getCollections().getFirst().getFields().getFirst().getId(),
                indexManager.getIndexId()
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
    @Test
    public void test_SchemeManager_WithData() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        IndexStorageManagerFactory indexStorageManagerFactory = new DefaultIndexStorageManagerFactory(this.engineConfig, new JsonIndexHeaderManager.Factory());
        FieldIndexManagerProvider fieldIndexManagerProvider = new DefaultFieldIndexManagerProvider(engineConfig, indexStorageManagerFactory);

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
                                                                .primary(true)
                                                                .type("int")
                                                                .name("pk")
                                                                .build(),
                                                        Scheme.Field.builder()
                                                                .id(2)
                                                                .primary(false)
                                                                .type("int")
                                                                .name("age")
                                                                .meta(
                                                                        Scheme.Meta.builder()
                                                                                .min(15)
                                                                                .max(60)
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
                SchemeManager.SchemeUpdateConfig.builder().build(),
                fieldIndexManagerProvider,
                this.databaseStorageManager
        );
        schemeManager.update();

        Path path = Path.of(engineConfig.getBaseDBPath(), "scheme.json");
        Assertions.assertTrue(Files.exists(path));

        // --- ADD DATA TO THE COLLECTION --- //
        byte[] data = new byte[]{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01};
        Pointer pointer = this.databaseStorageManager.store(1, schemeManager.getScheme().getVersion(), data);

        // --- READING DATA AND VERIFYING DB CAN RETURN PROPER OBJ --- //
        DBObject dbObject = this.databaseStorageManager.select(pointer).get();
        byte[] valueOfField = CollectionSerializationUtil.getValueOfField(
                scheme.getCollections().getFirst(),
                scheme.getCollections().getFirst().getFields().getFirst(),
                dbObject
        );
        int i = Ints.fromByteArray(valueOfField);
        Assertions.assertEquals(1, i);
        Assertions.assertEquals(1, dbObject.getVersion());

        // --- ADDING THE DATA TO INDEX, WHICH IS CLUSTER INDEX AND IS AVAILABLE TO MANAGER TOO --- //
        IndexManager<Integer, Pointer> indexManager = (IndexManager<Integer, Pointer>) fieldIndexManagerProvider.getIndexManager(
                scheme.getCollections().getFirst(),
                scheme.getCollections().getFirst().getFields().getFirst()
        );
        indexManager.addIndex(1, pointer);
        Optional<Pointer> optionalPointer = indexManager.getIndex(1);
        Assertions.assertTrue(optionalPointer.isPresent());
        Assertions.assertEquals(pointer, optionalPointer.get());


        // --- UPDATING SCHEME, ADD NEW FIELD TO THE END OF COLLECTION --- //
        scheme.setVersion(2);
        ArrayList<Scheme.Field> fields = new ArrayList<>(scheme.getCollections().getFirst().getFields());
        Scheme.Field newField = Scheme.Field.builder()
                .type(FieldType.INT.getName())
                .name("balance")
                .defaultValue("15")
                .build();
        fields.add(newField);
        scheme.getCollections().getFirst().setFields(fields);

        schemeManager = new SchemeManager(
                engineConfig,
                scheme,
                SchemeManager.SchemeUpdateConfig.builder().build(),
                fieldIndexManagerProvider,
                this.databaseStorageManager
        );
        schemeManager.update();

        // --- SELECTING OBJECT USING OLD POINTER TO MAKE SURE ITS CHANGED AND IS NO LONGER ALIVE --- //
        dbObject = this.databaseStorageManager.select(pointer).get();
        Assertions.assertFalse(dbObject.isAlive());
        optionalPointer = indexManager.getIndex(1);
        Assertions.assertNotEquals(pointer, optionalPointer.get());

        // --- SELECTING UPDATED OBJECT AND CHECKING IF DEFAULT VALUE OF NEWLY ADDED FIELD IS SET --- //
        pointer = optionalPointer.get();
        dbObject = this.databaseStorageManager.select(pointer).get();

        valueOfField = CollectionSerializationUtil.getValueOfField(
                scheme.getCollections().getFirst(),
                newField,
                dbObject
        );
        i = Ints.fromByteArray(valueOfField);
        Assertions.assertEquals(15, i);

    }
}
