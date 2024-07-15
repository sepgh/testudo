package com.github.sepgh.test.storage.db;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.VerificationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.storage.db.DBObjectWrapper;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.db.Page;
import com.github.sepgh.testudo.storage.db.PageBuffer;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class DatabaseStorageManagerTestCase {

    private DatabaseStorageManager databaseStorageManager;
    private Path dbPath;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory("TEST_DatabaseStorageManagerTestCase");
        EngineConfig engineConfig = EngineConfig.builder()
                .baseDBPath(this.dbPath.toString())
                .build();
        this.databaseStorageManager = new DatabaseStorageManager(
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
    public void test_canStoreObject() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = this.databaseStorageManager.store(17, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        // Making sure page wrapper is no longer held in referenced pages
        PageBuffer pageBuffer = this.databaseStorageManager.getPageBuffer();
        Field referencedWrappers = PageBuffer.class.getDeclaredField("referencedWrappers");
        referencedWrappers.setAccessible(true);
        Map<PageBuffer.PageTitle, PageBuffer.PageWrapper> referencedPageWrappers = (Map<PageBuffer.PageTitle, PageBuffer.PageWrapper>) referencedWrappers.get(pageBuffer);
        Assertions.assertTrue(referencedPageWrappers.isEmpty());
    }

    @Test
    public void test_canStoreAndReadObject() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = this.databaseStorageManager.store(17, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        Optional<DBObjectWrapper> optionalDBObjectWrapper = this.databaseStorageManager.select(pointer);
        Assertions.assertTrue(optionalDBObjectWrapper.isPresent());

        DBObjectWrapper dbObjectWrapper = optionalDBObjectWrapper.get();
        Assertions.assertTrue(dbObjectWrapper.isAlive());
        Assertions.assertEquals(17, dbObjectWrapper.getCollectionId());
        String string = new String(dbObjectWrapper.getData(), StandardCharsets.UTF_8);
        Assertions.assertEquals("Test", string);
    }

    @Test
    public void test_canStoreUpdateAndReadObject() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = this.databaseStorageManager.store(17, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        this.databaseStorageManager.update(pointer, dbObjectWrapper -> {
            try {
                dbObjectWrapper.modifyData("Nest".getBytes(StandardCharsets.UTF_8));
            } catch (VerificationException.InvalidDBObjectWrapper e) {
                throw new RuntimeException(e);
            }
        });

        Optional<DBObjectWrapper> optionalDBObjectWrapper = this.databaseStorageManager.select(pointer);
        Assertions.assertTrue(optionalDBObjectWrapper.isPresent());

        DBObjectWrapper dbObjectWrapper = optionalDBObjectWrapper.get();
        Assertions.assertTrue(dbObjectWrapper.isAlive());
        Assertions.assertEquals(17, dbObjectWrapper.getCollectionId());
        String string = new String(dbObjectWrapper.getData(), StandardCharsets.UTF_8);
        Assertions.assertEquals("Nest", string);
    }

    @Test
    public void test_canStoreDeleteAndReuseObject() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = this.databaseStorageManager.store(17, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        this.databaseStorageManager.remove(pointer);
        pointer = this.databaseStorageManager.store(17, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());
    }

}
