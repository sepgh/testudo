package com.github.sepgh.test.storage.db;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.operation.DefaultFieldIndexManagerProvider;
import com.github.sepgh.testudo.operation.FieldIndexManagerProvider;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.SchemeManager;
import com.github.sepgh.testudo.storage.db.DiskPageDatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.DefaultIndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.IndexStorageManagerFactory;
import com.github.sepgh.testudo.storage.index.header.JsonIndexHeaderManager;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
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
import java.util.List;

public class SchemeManagerTestCase {

    private DiskPageDatabaseStorageManager diskPageDatabaseStorageManager;
    private Path dbPath;
    private EngineConfig engineConfig;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory("TEST_DatabaseStorageManagerTestCase");
        this.engineConfig = EngineConfig.builder()
                .clusterIndexKeyStrategy(EngineConfig.ClusterIndexKeyStrategy.INTEGER)
                .baseDBPath(this.dbPath.toString())
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
                this.diskPageDatabaseStorageManager
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
}
